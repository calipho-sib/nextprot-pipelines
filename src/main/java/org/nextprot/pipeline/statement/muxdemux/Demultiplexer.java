package org.nextprot.pipeline.statement.muxdemux;


import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.PipelineElement;
import org.nextprot.pipeline.statement.elements.BasePipelineElement;
import org.nextprot.pipeline.statement.elements.ElementEventHandler;
import org.nextprot.pipeline.statement.elements.Sink;
import org.nextprot.pipeline.statement.elements.runnable.BaseFlowablePipelineElement;
import org.nextprot.pipeline.statement.elements.runnable.FlowablePipelineElement;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * De-multiplexer receive statements via one source pipe and
 * load balance them to multiple sink pipes.
 */
public class Demultiplexer implements PipelineElement<DuplicableElement> {

	private boolean hasStarted;

	private final int sinkCapacity;
	private BlockingQueue<Statement> sinkPipePort;
	private final CircularList<BlockingQueue<Statement>> sourcePipePorts;
	private final List<DuplicableElement> nextElements;

	private final AtomicInteger incrementer = new AtomicInteger (-1);

	public Demultiplexer(int sinkCapacity, int sourcePipePortCount) {

		this.sinkCapacity = sinkCapacity;
		this.nextElements = new ArrayList<>();
		this.sourcePipePorts = createSourcePipePorts(sinkCapacity, sourcePipePortCount);

		if (sourcePipePorts.isEmpty()) {

			throw new IllegalArgumentException(getName()+": cannot create source ports");
		}
	}

	private CircularList<BlockingQueue<Statement>> createSourcePipePorts(int capacity, int sourcePipePortCount) {

		CircularList<BlockingQueue<Statement>> spp = new CircularList<>();

		int newCapacity = capacity / sourcePipePortCount;

		for (int i=0 ; i<sourcePipePortCount ; i++) {

			spp.add(new ArrayBlockingQueue<>(newCapacity));
		}

		return spp;
	}

	/**
	 * This method first duplicate the whole pipeline from the given element
	 *
	 * @param element a duplicable element
	 * @throws IOException
	 */
	@Override
	public void pipe(DuplicableElement element) {

		// 1. duplicate the whole chain from this element to sink
		List<DuplicableElement> duplicablePipeline = getPipelineDuplicableElementsFrom(element);

		for (int i = 0; i < sourcePipePorts.size(); i++) {

			BlockingQueue<Statement> port = sourcePipePorts.get(i);

			// copy elements until sink
			List<DuplicableElement> copiedElements = duplicablePipeline.stream()
					.map(elt -> elt.duplicate(port.remainingCapacity()))
					.collect(Collectors.toList());

			if (! (copiedElements.get(copiedElements.size()-1) instanceof Sink) ) {

				throw new IllegalArgumentException(getName()+": cannot demux from element "+element.getName() + ", the last element should be a Sink");
			}

			// ... -> F0(src)    (snk)F1 -> F2 -> .... -> SINK
			DuplicableElement head = pipe(copiedElements);

			nextElements.add(head);
		}
	}

	/**
	 * Pipe elements together
	 * @param elements
	 * @return the head of the pipeline
	 * @throws IOException
	 */
	private static DuplicableElement pipe(List<DuplicableElement> elements) {

		// connect all ...
		for (int i = 1; i < elements.size(); i++) {

			elements.get(i - 1).pipe(elements.get(i));
		}

		return elements.get(0);
	}

	private List<DuplicableElement> getPipelineDuplicableElementsFrom(DuplicableElement element) {

		List<DuplicableElement> pipelineElementList = new ArrayList<>();

		pipelineElementList.add(element);

		incrementer.set(0);

		while ((element = element.nextElement()) != null) {

			pipelineElementList.add(element);
		}

		return pipelineElementList;
	}

	@Override
	public void openValves(List<Thread> collector) {

		if (!hasStarted) {
			hasStarted = true;
			FlowablePipelineElement runnable = newFlowable();

			Thread thread = new Thread(runnable);
			thread.setName(runnable.getThreadName());
			thread.start();

			collector.add(thread);
		}

		// start the next elements into their own thread
		for (PipelineElement pipelineElement : nextElements) {
			pipelineElement.openValves(collector);
		}
	}

	@Override
	public FlowablePipelineElement newFlowable() {

		return new Flowable(this);
	}

	@Override
	public void closeValves() throws IOException {

		hasStarted = false;

		sinkPipePort.clear();
		createEventHandler().sinkUnpiped();

		for (PipelineElement outputPipelineElement : nextElements) {

			outputPipelineElement.closeValves();
		}
		createEventHandler().valvesClosed();
	}

	@Override
	public ElementEventHandler createEventHandler() throws FileNotFoundException {

		//return new ElementEventHandler.Mute();
		return new BasePipelineElement.ElementLog(getName());
	}

	public String getName() {
		return "Demux";
	}

	@Override
	public BlockingQueue<Statement> getSinkPipePort() {
		return sinkPipePort;
	}

	@Override
	public void setSinkPipePort(BlockingQueue<Statement> queue) {

		if (sinkCapacity != queue.remainingCapacity()) {

			throw new Error("Cannot set sink pipe with capacity "+queue.remainingCapacity() + " in port of capacity "+ sinkPipePort);
		}

		this.sinkPipePort = queue;
	}

	@Override
	public BlockingQueue<Statement> getSourcePipePort() {

		return sourcePipePorts.get(incrementer.incrementAndGet());
	}

	@Override
	public DuplicableElement nextElement() {

		//return nextElements.get(incrementer.get());
		throw new Error("Cannot call nextElement() on demux");
	}

	public static class CircularList<E> extends ArrayList<E> {

		@Override
		public E get(int index) {
			return super.get(index % size());
		}
	}

	private static class Flowable extends BaseFlowablePipelineElement<Demultiplexer> {

		public Flowable(Demultiplexer demultiplexer) {

			super(demultiplexer);
		}

		@Override
		public boolean handleFlow(Demultiplexer demultiplexer) throws Exception {

			Statement current = demultiplexer.getSinkPipePort().take();

			// 2. split in n output batch
			// 3. distribute to all output
			demultiplexer.getSourcePipePort().put(current);

			createEventHandler().statementHandled(current);

			return current == END_OF_FLOW_STATEMENT;
		}

//		printlnTextInLog("distributing " + numOfStatements + " statements...");
	}
}