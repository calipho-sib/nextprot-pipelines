package org.nextprot.pipeline.statement.elements;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.PipelineElement;
import org.nextprot.pipeline.statement.ports.SinkPipePort;
import org.nextprot.pipeline.statement.ports.SourcePipePort;

import java.io.IOException;
import java.util.List;


public abstract class BasePipelineElement<E extends PipelineElement> implements PipelineElement<E>, Runnable {

	public static final Statement END_OF_FLOW_TOKEN = null;

	private final int capacity;

	/**
	 * The ports through which pipeline elements communicate with each other.
	 * There exists
	 * - sink pipe port, through which Statements enter from the previous element
	 * - source pipe port, through which Statements exit to the next element.
	 * <p>
	 * It follows naturally that
	 * - source elements only contain source pipe ports
	 * - sink elements only contain sink pipe ports
	 * - and filter elements contain both.
	 */
	private SourcePipePort sourcePipePort;
	private SinkPipePort sinkPipePort;

	private boolean hasStarted;

	private E nextElement = null;

	public BasePipelineElement(int capacity) {

		this(capacity, new SinkPipePort(capacity), new SourcePipePort(capacity));
	}

	public BasePipelineElement(int capacity, SinkPipePort sinkPipePort, SourcePipePort sourcePipePort) {

		this.capacity = capacity;
		this.sinkPipePort = sinkPipePort;
		this.sourcePipePort = sourcePipePort;
	}

	/**
	 * Pipe this element with the next element
	 *
	 * @param nextElement
	 * @throws IOException
	 */
	@Override
	public void pipe(E nextElement) throws IOException {

		this.nextElement = nextElement;
		sourcePipePort.connect(nextElement.getSinkPipePort());
	}

	@Override
	public E nextElement() {
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
	public SinkPipePort getSinkPipePort() {

		return sinkPipePort;
	}

	@Override
	public SourcePipePort getSourcePipePort() {

		return sourcePipePort;
	}

	public void start(List<Thread> collector) {

		if (!hasStarted) {
			hasStarted = true;
			Thread thread = new Thread(this, getName());
			thread.start();
			collector.add(thread);
			System.out.println(getName() + ": activated (capacity=" + capacity + ")");
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
				System.err.println(getName() + ": could not stop, e=" + e.getMessage());
			}
		}
	}

	protected void endOfFlow() {

		System.out.println(getName() + ": end of flow");
	}

	public void stop() throws IOException {

		if (sinkPipePort != null) {
			sinkPipePort.close();
			System.out.println(Thread.currentThread().getName() + ": sink pipe port closed");
		}
		if (sourcePipePort != null) {
			sourcePipePort.close();
			System.out.println(Thread.currentThread().getName() + ": source pipe port closed");
		}
		System.out.println(getName() + ": stopped");
	}

	protected abstract void handleFlow() throws IOException;
}
