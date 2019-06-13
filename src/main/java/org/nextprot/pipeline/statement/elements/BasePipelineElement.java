package org.nextprot.pipeline.statement.elements;

import org.nextprot.pipeline.statement.PipelineElement;
import org.nextprot.pipeline.statement.elements.runnable.RunnablePipelineElement;
import org.nextprot.pipeline.statement.ports.SinkPipePort;
import org.nextprot.pipeline.statement.ports.SourcePipePort;

import java.io.IOException;
import java.util.List;


public abstract class BasePipelineElement<E extends PipelineElement> implements PipelineElement<E> {

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

	public BasePipelineElement(SinkPipePort sinkPipePort, SourcePipePort sourcePipePort) {

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
	public String getName() {

		return getClass().getSimpleName();
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
	public void run(List<Thread> collector) {

		if (!hasStarted) {
			hasStarted = true;

			RunnablePipelineElement runnable = newRunnableElement();

			Thread thread = new Thread(runnable);
			thread.setName(runnable.getThreadName());
			thread.start();

			collector.add(thread);
		}

		if (nextElement != null) {
			nextElement.run(collector);
		}
	}

	@Override
	public void unpipe() throws IOException {

		hasStarted = false;

		if (sinkPipePort != null) {

			sinkPipePort.close();
			sinkPipePortUnpiped();
		}
		if (sourcePipePort != null) {

			sourcePipePort.close();
			sourcePipePortUnpiped();
		}
		elementClosed();
	}

	@Override
	public void sinkPipePortUnpiped() { }

	@Override
	public void sourcePipePortUnpiped() { }

	@Override
	public void elementClosed() { }
}
