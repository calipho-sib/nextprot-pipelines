package org.nextprot.pipeline.statement.muxdemux;


import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.PipelineElement;
import org.nextprot.pipeline.statement.elements.Sink;
import org.nextprot.pipeline.statement.ports.SinkPipePort;
import org.nextprot.pipeline.statement.ports.SourcePipePort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * De-multiplexer receive statements via one source pipe and
 * load balance them to multiple sink pipes.
 */
public class Demultiplexer implements PipelineElement<DuplicableElement>, Runnable {

	private boolean hasStarted;

	private final SinkPipePort sinkPipePort;
	private final CircularList<SourcePipePort> sourcePipePorts;
	private final List<DuplicableElement> nextElements;

	public AtomicInteger incrementer = new AtomicInteger (0);

	public Demultiplexer(SinkPipePort sinkPipePort, int sourcePipePortCount) {

		this.sinkPipePort = sinkPipePort;
		this.nextElements = new ArrayList<>();
		this.sourcePipePorts = createSourcePipePorts(sinkPipePort.capacity(), sourcePipePortCount);

		if (sourcePipePorts.isEmpty()) {

			throw new IllegalArgumentException(getName()+": cannot create src ports");
		}
	}

	private final CircularList<SourcePipePort> createSourcePipePorts(int capacity, int sourcePipePortCount) {

		CircularList<SourcePipePort> spp = new CircularList<>();

		int newCapacity = capacity / sourcePipePortCount;

		for (int i=0 ; i<sourcePipePortCount ; i++) {

			spp.add(new SourcePipePort(newCapacity));
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
	public void pipe(DuplicableElement element) throws IOException {

		for (SourcePipePort srcPort : sourcePipePorts) {

			if (srcPort.isConnected()) {

				throw new IllegalArgumentException(getName()+": src port is already connected");
			}
		}

		// 1. duplicate the whole chain from this element to sink
		List<DuplicableElement> duplicablePipeline = getPipelineDuplicableElementsFrom(element);

		for (int i = 0; i < sourcePipePorts.size(); i++) {

			SourcePipePort port = sourcePipePorts.get(i);

			// copy elements until sink
			List<DuplicableElement> copiedElements = duplicablePipeline.stream()
					.map(elt -> elt.duplicate(port.capacity()))
					.collect(Collectors.toList());

			if (! (copiedElements.get(copiedElements.size()-1) instanceof Sink) ) {

				throw new IllegalArgumentException(getName()+": cannot demux from element "+element.getName() + ", the last element should be a Sink");
			}

			// ... -> F0(src)    (snk)F1 -> F2 -> .... -> SINK
			DuplicableElement first = connect(copiedElements);

			// ... -> F0(src) -> (snk)F1 -> F2 -> .... -> SINK
			port.connect(first.getSinkPipePort());

			nextElements.add(first);
		}
	}

	private static DuplicableElement connect(List<DuplicableElement> elements) throws IOException {

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
	public void start(List<Thread> collector) {

		if (!hasStarted) {
			hasStarted = true;
			Thread thread = new Thread(this, getName());
			thread.start();
			collector.add(thread);
			System.out.println(getName() + ": opened (capacity=" + sinkPipePort.capacity() + ")");
		}

		for (PipelineElement pipelineElement : nextElements) {
			pipelineElement.start(collector);
		}
	}

	@Override
	public boolean hasStarted() {
		return hasStarted;
	}

	@Override
	public void stop() throws IOException {

		sinkPipePort.close();
		System.out.println(Thread.currentThread().getName() + ": sink pipe closed");

		System.out.println(Thread.currentThread().getName() + ": input port closed");

		for (PipelineElement outputPipelineElement : nextElements) {
			outputPipelineElement.getSourcePipePort().close();
			System.out.println(outputPipelineElement.getName() + ": output port closed");
		}
	}

	@Override
	public void run() {

		try {
			// 1. get input
			Statement[] buffer = new Statement[sinkPipePort.capacity()];

			int numOfStatements = sinkPipePort.read(buffer, 0, sinkPipePort.capacity());

			int j = 0;
			for (int i = 0; i < numOfStatements; i++) {

				// 2. split in n output batch
				// 3. distribute to all output
				sourcePipePorts.get(j++).write(buffer[i]);

				System.out.println(Thread.currentThread().getName()
						+ ": transmit statement " + buffer[i].getStatementId());
			}
		} catch (IOException e) {
			System.err.println(e.getMessage() + " in thread " + Thread.currentThread().getName());
		}
		// When done with the data, close the pipe and flush the Writer
		finally {
			try {
				stop();
			} catch (IOException e) {
				System.err.println(Thread.currentThread().getName() + ": could not close the pipe, e=" + e.getMessage());
			}
		}
	}

	public String getName() {
		return "Demux";
	}

	@Override
	public SinkPipePort getSinkPipePort() {
		return sinkPipePort;
	}

	@Override
	public SourcePipePort getSourcePipePort() {

		return null;
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
}
