package org.nextprot.pipeline.statement.elements;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.PipelineElement;
import org.nextprot.pipeline.statement.pipes.SinkPipe;
import org.nextprot.pipeline.statement.pipes.SourcePipe;

import java.io.IOException;
import java.util.List;


public abstract class BasePipelineElement implements PipelineElement, Runnable {

	public static final Statement END_OF_FLOW_TOKEN = null;

	private final int capacity;

	/**
	 * The ports through which pipeline elements communicate with each other are called pipes.
	 * There exists
	 * - sink pipe, through which Statements enter an element
	 * - source pipe, through which Statements exit an element.
	 * <p>
	 * It follows naturally that
	 * - source elements only contain source pipes
	 * - sink elements only contain sink pipes
	 * - and filter elements contain both.
	 */
	private SourcePipe sourcePipe;
	private SinkPipe sinkPipe;

	private boolean hasStarted;

	private PipelineElement nextElement = null;

	public BasePipelineElement(int capacity) {
		this.capacity = capacity;
	}

	public BasePipelineElement(int capacity, SinkPipe sinkPipe) {
		this(capacity);
		this.sinkPipe = sinkPipe;
	}

	/**
	 * Connect this pipe with the sink element
	 *
	 * @param nextElement
	 * @throws IOException
	 */
	@Override
	public void connect(PipelineElement nextElement) throws IOException {

		this.nextElement = nextElement;
		sourcePipe = new SourcePipe();
		sourcePipe.connect(nextElement.getSinkPipe());
	}

	@Override
	public PipelineElement nextElement() {
		return nextElement;
	}

	@Override
	public int getCapacity() {

		return capacity;
	}

	/**
	 * This protected method requests a Pipe threads to create and return
	 * a PipedInputPort thread so that another Pipe thread can connect to it.
	 **/
	@Override
	public SinkPipe getSinkPipe() {

		return sinkPipe;
	}

	@Override
	public SourcePipe getSourcePipe() {

		return sourcePipe;
	}

	public void start(List<Thread> collector) {

		if (!hasStarted) {
			hasStarted = true;
			Thread thread = new Thread(this, getName());
			thread.start();
			collector.add(thread);
			System.out.println("Element " + getName() + ": activated (capacity=" + capacity + ")");
		}

		if (nextElement != null) {
			nextElement.start(collector);
		}
	}

	@Override
	public boolean hasStarted() {

		return hasStarted;
	}

	@Override
	public void run() {

		try {
			handleFlow();
			endOfFlow();
		} catch (IOException e) {
			System.err.println(e.getMessage() + " in thread " + Thread.currentThread().getName());
		}
		// When done with the data, close the pipe and flush the Writer
		finally {
			try {
				stop();
			} catch (IOException e) {
				System.err.println("Element " + getName() + ": could not stop, e=" + e.getMessage());
			}
		}
	}

	protected void endOfFlow() {

		System.out.println("Element " + getName() + ": end of flow");
	}

	public void stop() throws IOException {

		if (sinkPipe != null) {
			sinkPipe.close();
			System.out.println(Thread.currentThread().getName() + ": sink pipe closed");
		}
		if (sourcePipe != null) {
			sourcePipe.close();
			System.out.println(Thread.currentThread().getName() + ": source pipe closed");
		}
		System.out.println("Element " + getName() + ": stopped");
	}

	protected abstract void handleFlow() throws IOException;

	public static PipelineElement connect(List<PipelineElement> elements) throws IOException {

		// connect all ...
		for (int i = 1; i < elements.size(); i++) {

			elements.get(i - 1).connect(elements.get(i));
		}
		return elements.get(0);
	}
}
