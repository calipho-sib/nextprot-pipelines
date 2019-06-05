package org.nextprot.pipeline.statement.muxdemux;


import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.PipelineElement;
import org.nextprot.pipeline.statement.elements.BasePipelineElement;
import org.nextprot.pipeline.statement.elements.Sink;
import org.nextprot.pipeline.statement.pipes.SinkPipe;
import org.nextprot.pipeline.statement.pipes.SourcePipe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * De-multiplexer receive statements via one source pipe and
 * load balance them to multiple sink pipes.
 */
public class Demultiplexer implements PipelineElement, Runnable {

	private boolean hasStarted;

	private final int capacity;
	private final SinkPipe sinkPipe;
	private final int duplication;
	private final CircularList<PipelineElement> nextElements;

	public AtomicInteger incrementer = new AtomicInteger (0);

	public Demultiplexer(int capacity, int duplication) {

		this.capacity = capacity;
		this.sinkPipe = new SinkPipe(capacity);
		this.duplication = duplication;
		nextElements = new CircularList<>();
	}

	@Override
	public void connect(PipelineElement element) throws IOException {

		if (!nextElements.isEmpty()) {

			throw new IllegalArgumentException("sink elements have been already connected");
		}

		List<DuplicableElement> duplicableElements = getPipelineDuplicableElementsFrom(element);

		// 1. duplicate the whole chain from this element to sink
		// 2. add each duplicated element into a list

		int newCapacity = capacity / duplication;

		for (int i = 0; i < duplication; i++) {

			// copy elements until sink
			List<PipelineElement> copiedElements = duplicableElements.stream()
					.map(elt -> elt.duplicate(newCapacity))
					.collect(Collectors.toList());

			if (! (copiedElements.get(copiedElements.size()-1) instanceof Sink) ) {

				throw new IllegalArgumentException("Missing a Sink element from element "+element.getName());
			}

			PipelineElement first = BasePipelineElement.connect(copiedElements);
			nextElements.add(first);
		}
	}

	private List<DuplicableElement> getPipelineDuplicableElementsFrom(PipelineElement element) {

		List<DuplicableElement> pipelineElementList = new ArrayList<>();

		if (element instanceof DuplicableElement) {
			pipelineElementList.add((DuplicableElement) element);
		}
		while ((element = element.nextElement()) != null) {

			if (element instanceof DuplicableElement) {
				pipelineElementList.add((DuplicableElement) element);
			} else {
				break;
			}
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
			System.out.println("Pipe " + getName() + ": opened (capacity=" + capacity + ")");
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

		sinkPipe.close();
		System.out.println(Thread.currentThread().getName() + ": sink pipe closed");

		System.out.println(Thread.currentThread().getName() + ": input port closed");

		for (PipelineElement outputPipelineElement : nextElements) {
			outputPipelineElement.getSourcePipe().close();
			System.out.println(outputPipelineElement.getName() + ": output port closed");
		}
	}

	@Override
	public void run() {

		try {
			// 1. get input
			Statement[] buffer = new Statement[getCapacity()];

			int numOfStatements = sinkPipe.read(buffer, 0, getCapacity());

			int j = 0;
			for (int i = 0; i < numOfStatements; i++) {

				// 2. split in n output batch
				// 3. distribute to all output
				nextElements.get(j++).getSourcePipe().write(buffer[i]);

				System.out.println(Thread.currentThread().getName()
						+ ": filter statement " + buffer[i].getStatementId());
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
	public SinkPipe getSinkPipe() {
		return sinkPipe;
	}

	@Override
	public SourcePipe getSourcePipe() {

		return null;
	}

	@Override
	public int getCapacity() {
		return capacity;
	}

	@Override
	public PipelineElement nextElement() {
		;
		return nextElements.get(incrementer.incrementAndGet());
	}

	public static class CircularList<E> extends ArrayList<E> {

		@Override
		public E get(int index) {
			return super.get(index % size());
		}
	}
}
