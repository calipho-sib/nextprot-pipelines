package org.nextprot.pipeline.statement.core.elements.demux;


import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.core.elements.Demux;
import org.nextprot.pipeline.statement.core.PipelineElement;
import org.nextprot.pipeline.statement.core.elements.BasePipelineElement;
import org.nextprot.pipeline.statement.core.elements.ElementEventHandler;
import org.nextprot.pipeline.statement.core.elements.Sink;
import org.nextprot.pipeline.statement.core.elements.flowable.BaseFlowLog;
import org.nextprot.pipeline.statement.core.elements.flowable.BaseFlowablePipelineElement;
import org.nextprot.pipeline.statement.core.elements.flowable.FlowEventHandler;
import org.nextprot.pipeline.statement.core.elements.flowable.FlowablePipelineElement;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.nextprot.pipeline.statement.core.elements.Source.POISONED_STATEMENT;

/**
 * De-multiplexer receive statements via one source pipe and
 * load balance them to multiple sink pipes.
 */
public class Demultiplexer implements Demux, PipelineElement<DuplicableElement> {

	private boolean valvesOpened;

	private final int sinkCapacity;
	private BlockingQueue<Statement> sinkChannel;
	private final CircularList<BlockingQueue<Statement>> sourceChannels;
	private final List<DuplicableElement> nextConnectedSinks;
	private final ElementEventHandler eventHandler;

	private final AtomicInteger incrementer = new AtomicInteger (-1);

	public Demultiplexer(int sinkCapacity, int sourceChannelCount) {

		this.sinkCapacity = sinkCapacity;
		this.nextConnectedSinks = new ArrayList<>();
		this.sourceChannels = createSourceChannels(sinkCapacity, sourceChannelCount);

		if (sourceChannels.isEmpty()) {

			throw new IllegalArgumentException(getName()+": cannot create source channels");
		}

		try {
			eventHandler = createElementEventHandler();
		} catch (FileNotFoundException e) {
			throw new IllegalStateException(e);
		}
	}

	private CircularList<BlockingQueue<Statement>> createSourceChannels(int capacity, int channelCount) {

		CircularList<BlockingQueue<Statement>> sources = new CircularList<>();

		int newCapacity = capacity / channelCount;

		if (newCapacity <= 0) {
			throw new IllegalStateException("indivisible capacity: original source channel of capacity "+capacity
					+" cannot be divide in "+channelCount+ " channels");
		}

		for (int i=0 ; i<channelCount ; i++) {

			sources.add(new ArrayBlockingQueue<>(newCapacity));
		}

		return sources;
	}

	/**
	 * Duplicates the whole pipeline from the given head element and
	 *
	 * @param head a duplicable element
	 *
	 * State before this method is called:
	 *
	 * 		 --- SOURCE_-1 -- DEMUX --- [ HEAD_0   -> FILTER_1   -> FILTER_2   -> .... -> SINK_N   ]
	 *
	 * State after this method is called:
	 *
	 * 		                     ------ [ HEAD_0.1 -> FILTER_1.1 -> FILTER_2.1 -> .... -> SINK_N.1 ]
	 * 		                    | ----- [ HEAD_0.2 -> FILTER_1.2 -> FILTER_2.2 -> .... -> SINK_N.2 ]
	 * 		 --- SOURCE_-1 -- DEMUX --- [ HEAD_0.3 -> FILTER_1.3 -> FILTER_2.3 -> .... -> SINK_N.3 ]
	 * 		                    | ----- [ HEAD_0.4 -> FILTER_1.4 -> FILTER_2.4 -> .... -> SINK_N.4 ]
	 * 		                     ------ [ HEAD_0.5 -> FILTER_1.5 -> FILTER_2.5 -> .... -> SINK_N.5 ]
	 * 		                     -XXXX- [ HEAD_0      FILTER_1      FILTER_2      ....    SINK_N   ]
	 */
	@Override
	public void pipe(DuplicableElement head) {

		List<DuplicableElement> originalElements = getElementsFromHead(head);

		for (int i = 0; i < sourceChannels.size(); i++) {

			BlockingQueue<Statement> sourceChannel = sourceChannels.get(i);

			DuplicableElement duplicatedHead =
					duplicateAndPipe(originalElements, sourceChannel.remainingCapacity());

			duplicatedHead.setSinkChannel(sourceChannel);

			nextConnectedSinks.add(duplicatedHead);
		}

		// unpipe original (TODO: make elements eligible for GC)
		originalElements.forEach(element -> element.setSinkChannel(null));
	}

	/**
	 * Duplicate elements from HEAD to SINK and pipe them
	 *
	 * @param originalElements original elements
	 * @param capacity the channels capacity
	 * @return the head element
	 */
	private DuplicableElement duplicateAndPipe(List<DuplicableElement> originalElements, int capacity) {

		// 1. Duplicate elements from HEAD to SINK
		List<DuplicableElement> duplicatedElements = originalElements.stream()
				.map(elt -> elt.duplicate(capacity))
				.collect(Collectors.toList());

		if (! (duplicatedElements.get(duplicatedElements.size()-1) instanceof Sink) ) {

			throw new IllegalArgumentException(getName()+": cannot demux from HEAD element "+
					originalElements.get(0).getName() + ", the last element should be a SINK");
		}

		return pipeTogether(duplicatedElements);
	}

	/**
	 * Pipe elements together
	 * @param elements the elements to pipe
	 * @return the head of the new pipeline
	 */
	private static DuplicableElement pipeTogether(List<DuplicableElement> elements) {

		if (elements.isEmpty()) {

			throw new IllegalArgumentException("cannot pipe empty elements");
		}

		for (int i = 1; i < elements.size(); i++) {

			elements.get(i - 1).pipe(elements.get(i));
		}

		return elements.get(0);
	}

	private List<DuplicableElement> getElementsFromHead(DuplicableElement head) {

		List<DuplicableElement> pipelineElementList = new ArrayList<>();

		pipelineElementList.add(head);

		DuplicableElement element = head;

		while ((element = element.nextSink()) != null) {

			pipelineElementList.add(element);
		}

		return pipelineElementList;
	}

	@Override
	public void openValves(List<Thread> collector) {

		if (!valvesOpened) {
			valvesOpened = true;

			FlowablePipelineElement runnable = newFlowable();

			Thread thread = new Thread(runnable);
			thread.setName(runnable.getThreadName());
			thread.start();

			eventHandler.valvesOpened();

			collector.add(thread);
		}

		// start the next elements into their own thread
		for (PipelineElement sink : nextConnectedSinks) {
			sink.openValves(collector);
		}
	}

	@Override
	public void closeValves() throws IOException {

		valvesOpened = false;

		eventHandler.valvesClosed();

		eventHandler.sinkUnpiped();

		for (PipelineElement sink : nextConnectedSinks) {

			sink.closeValves();
		}
	}

	@Override
	public FlowablePipelineElement newFlowable() {

		return new Flowable(this);
	}

	private ElementEventHandler createElementEventHandler() throws FileNotFoundException {

		//return new ElementEventHandler.Mute();
		return new BasePipelineElement.ElementLog(getName());
	}

	public String getName() {
		return "Demux";
	}

	@Override
	public BlockingQueue<Statement> getSinkChannel() {
		return sinkChannel;
	}

	@Override
	public void setSinkChannel(BlockingQueue<Statement> channel) {

		if (sinkCapacity != channel.remainingCapacity()) {

			throw new Error("Cannot set sink channel with capacity "+channel.remainingCapacity() + " in channel port of capacity "+ sinkChannel.remainingCapacity());
		}

		this.sinkChannel = channel;
	}

	@Override
	public BlockingQueue<Statement> getSourceChannel() {

		return sourceChannels.get(incrementer.incrementAndGet());
	}

	public int countSourceChannels() {
		return sourceChannels.size();
	}

	@Override
	public DuplicableElement nextSink() {

		return null;
	}

	private static class CircularList<E> extends ArrayList<E> {

		@Override
		public E get(int index) {
			return super.get(index % size());
		}
	}

	private static class Flowable extends BaseFlowablePipelineElement<Demultiplexer> {

		private int poisonedStatementReceived = 0;

		public Flowable(Demultiplexer demultiplexer) {

			super(demultiplexer);
		}

		@Override
		public boolean handleFlow(Demultiplexer demultiplexer) throws Exception {

			Statement current = demultiplexer.getSinkChannel().take();

			BlockingQueue<Statement> sourceChannel = demultiplexer.getSourceChannel();

			sourceChannel.put(current);

			((FlowLog)getFlowEventHandler()).statementHandled(current, demultiplexer.sinkChannel, sourceChannel);

			if (current == POISONED_STATEMENT) {
				poisonedStatementReceived++;
			}

			return poisonedStatementReceived == demultiplexer.countSourceChannels();
		}

		@Override
		protected FlowEventHandler createFlowEventHandler() throws FileNotFoundException {

			return new FlowLog(getThreadName());
		}
	}

	private static class FlowLog extends BaseFlowLog {

		private FlowLog(String threadName) throws FileNotFoundException {

			super(threadName);
		}

		@Override
		public void beginOfFlow() {

			sendMessage("start distribution");
		}

		private void statementHandled(Statement statement, BlockingQueue<Statement> sinkChannel,
		                             BlockingQueue<Statement> sourceChannel) {

			statementHandled("distributing", statement, sinkChannel, sourceChannel);
		}

		@Override
		public void endOfFlow() {

			sendMessage(getStatementCount()+" healthy statements distributed");
		}
	}
}