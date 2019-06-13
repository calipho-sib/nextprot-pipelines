package org.nextprot.pipeline.statement.elements;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.PipelineElement;
import org.nextprot.pipeline.statement.ports.SinkPipePort;
import org.nextprot.pipeline.statement.ports.SourcePipePort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public abstract class BasePipelineElement<E extends PipelineElement> implements PipelineElement<E>, EventHandler {

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
	private final SourcePipePort sourcePipePort;
	private final SinkPipePort sinkPipePort;

	private boolean hasStarted;

	private E nextElement = null;

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

	@Override
	public void start(List<Thread> collector) {

		if (!hasStarted) {
			hasStarted = true;
			Thread thread = new Thread(this, getThreadName());
			thread.start();
			collector.add(thread);
		}

		if (nextElement != null) {
			nextElement.start(collector);
		}
	}

	@Override
	public void run() {

		try {
			List<Statement> buffer = new ArrayList<>();
			elementOpened(capacity);

			boolean endOfFlow = false;

			while (!endOfFlow) {

				endOfFlow = handleFlow(buffer);
				buffer.clear();
			}
			endOfFlow();
		} catch (IOException e) {
			System.err.println(Thread.currentThread().getName() +": "+e.getMessage());
		}
		finally {
			try {
				stop();
			} catch (IOException e) {
				System.err.println(Thread.currentThread().getName() + ": could not stop, e=" + e.getMessage());
			}
		}
	}

	public void stop() throws IOException {

		hasStarted = false;

		if (sinkPipePort != null) {

			sinkPipePort.close();
			sinkPipePortClosed();
		}
		if (sourcePipePort != null) {

			sourcePipePort.close();
			sourcePipePortClosed();
		}
		elementClosed();
	}

	//public void elementOpened(int capacity) {}
	//public void endOfFlow() {}
	public void sinkPipePortClosed() {}
	public void sourcePipePortClosed() {}
	public void elementClosed() {}
}
