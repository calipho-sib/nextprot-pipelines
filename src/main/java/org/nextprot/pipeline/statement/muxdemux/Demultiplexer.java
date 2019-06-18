package org.nextprot.pipeline.statement.muxdemux;


import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.PipelineElement;
import org.nextprot.pipeline.statement.elements.BasePipelineElement;
import org.nextprot.pipeline.statement.elements.ElementEventHandler;
import org.nextprot.pipeline.statement.elements.Sink;
import org.nextprot.pipeline.statement.elements.runnable.BaseRunnablePipelineElement;
import org.nextprot.pipeline.statement.elements.runnable.RunnablePipelineElement;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * De-multiplexer receive statements via one source pipe and
 * load balance them to multiple sink pipes.
 */
public class Demultiplexer implements PipelineElement<DuplicableElement> {

	private boolean hasStarted;

	private BlockingQueue<Statement> sinkPipePort;
	private final CircularList<BlockingQueue<Statement>> sourcePipePorts;
	private final List<DuplicableElement> nextElements;

	private final AtomicInteger incrementer = new AtomicInteger (0);

	public Demultiplexer(BlockingQueue<Statement> sinkPipePort, int sourcePipePortCount) {

		this.sinkPipePort = sinkPipePort;
		this.nextElements = new ArrayList<>();
		this.sourcePipePorts = createSourcePipePorts(sinkPipePort.size(), sourcePipePortCount);

		if (sourcePipePorts.isEmpty()) {

			throw new IllegalArgumentException(getName()+": cannot create src ports");
		}
	}

	private CircularList<BlockingQueue<Statement>> createSourcePipePorts(int capacity, int sourcePipePortCount) {

		CircularList<BlockingQueue<Statement>> spp = new CircularList<>();

		int newCapacity = capacity / sourcePipePortCount;

		for (int i=0 ; i<sourcePipePortCount ; i++) {

			spp.add(new LinkedBlockingQueue<>(newCapacity));
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
					.map(elt -> elt.duplicate(port.size()))
					.collect(Collectors.toList());

			if (! (copiedElements.get(copiedElements.size()-1) instanceof Sink) ) {

				throw new IllegalArgumentException(getName()+": cannot demux from element "+element.getName() + ", the last element should be a Sink");
			}

			// ... -> F0(src)    (snk)F1 -> F2 -> .... -> SINK
			DuplicableElement first = pipe(copiedElements);

			// ... -> F0(src) -> (snk)F1 -> F2 -> .... -> SINK
			//port.connect(first.getSinkPipePort());

			nextElements.add(first);
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

		while ((element = element.nextElement()) != null) {

			pipelineElementList.add(element);
		}

		return pipelineElementList;
	}

	@Override
	public void openValves(List<Thread> collector) {

		if (!hasStarted) {
			hasStarted = true;
			RunnablePipelineElement runnable = newRunnableElement();

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
	public RunnablePipelineElement newRunnableElement() {

		return new Runnable(this);
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

		throw new Error("Already connected to a source PipelineElement through this pipe!");
	}

	@Override
	public BlockingQueue<Statement> getSourcePipePort() {

		return null;
	}

	public BlockingQueue<Statement> getSourcePipePort(int i) {

		return sourcePipePorts.get(i);
	}

	@Override
	public DuplicableElement nextElement() {

		return nextElements.get(incrementer.incrementAndGet());
	}

	public static class CircularList<E> extends ArrayList<E> {

		@Override
		public E get(int index) {
			return super.get(index % size());
		}
	}

	private static class Runnable extends BaseRunnablePipelineElement<Demultiplexer> {

		public Runnable(Demultiplexer demultiplexer) {

			super(demultiplexer);
		}

		@Override
		public boolean handleFlow() throws Exception {

			Demultiplexer pipelineElement = getPipelineElement();

			BlockingQueue<Statement> sinkPipePort = pipelineElement.getSinkPipePort();

			// 1. get input
			List<Statement> buffer = new ArrayList<>(sinkPipePort.size());

			sinkPipePort.drainTo(buffer);

			int j = 0;
			for (int i = 0; i < buffer.size(); i++) {

				Statement current = buffer.get(i);

				// 2. split in n output batch
				// 3. distribute to all output
				pipelineElement.getSourcePipePort(j++).put(current);

				createEventHandler().statementHandled(current);
			}

			return buffer.get(buffer.size()-1) == END_OF_FLOW_TOKEN;
		}

//		printlnTextInLog("distributing " + numOfStatements + " statements...");
	}
}