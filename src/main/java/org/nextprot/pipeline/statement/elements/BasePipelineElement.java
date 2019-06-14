package org.nextprot.pipeline.statement.elements;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.PipelineElement;
import org.nextprot.pipeline.statement.elements.runnable.RunnablePipelineElement;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public abstract class BasePipelineElement<E extends PipelineElement> implements PipelineElement<E> {

	private final ElementEventHandler eventHandler;

	private boolean hasStarted;
	private E nextElement = null;

	private final BlockingQueue<Statement> sourcePipePort;
	private BlockingQueue<Statement> sinkPipePort;

	public BasePipelineElement(int sourceCapacity) {

		this(new LinkedBlockingQueue<>(sourceCapacity));
	}

	public BasePipelineElement(BlockingQueue<Statement> sourcePipePort) {

		this.sourcePipePort = sourcePipePort;

		try {
			eventHandler = createEventHandler();
		} catch (FileNotFoundException e) {
			throw new IllegalStateException(e);
		}
	}

	/**
	 * Pipe this element with the next element
	 *
	 * @param nextElement
	 * @throws IOException
	 */
	@Override
	public void pipe(E nextElement) {

		this.nextElement = nextElement;
		nextElement.setSinkPipePort(sourcePipePort);
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
	public BlockingQueue<Statement> getSinkPipePort() {

		return sinkPipePort;
	}

	@Override
	public void setSinkPipePort(BlockingQueue<Statement> queue) {

		this.sinkPipePort = queue;
	}

	@Override
	public BlockingQueue<Statement> getSourcePipePort() {

		return sourcePipePort;
	}

	@Override
	public ElementEventHandler createEventHandler() throws FileNotFoundException {

		//return new ElementEventHandler.Mute();
		return new ElementLog(getName());
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

			sinkPipePort.clear(); // drained
			eventHandler.sinkPipePortUnpiped();
		}
		if (sourcePipePort != null) {

			sourcePipePort.clear(); // drained
			eventHandler.sourcePipePortUnpiped();
		}
		eventHandler.elementClosed();
	}

	public static class ElementLog extends BaseLog implements ElementEventHandler {

		public ElementLog(String threadName) throws FileNotFoundException {

			super(threadName, "logs");
		}

		@Override
		public void sinkPipePortUnpiped() {

			sendMessage("sink port unpiped");
		}

		@Override
		public void sourcePipePortUnpiped() {

			sendMessage("source port unpiped");
		}

		@Override
		public void elementClosed() {

			sendMessage("element closed");
		}
	}
}
