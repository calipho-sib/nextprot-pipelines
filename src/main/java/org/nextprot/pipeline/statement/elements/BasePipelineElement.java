package org.nextprot.pipeline.statement.elements;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.PipelineElement;
import org.nextprot.pipeline.statement.elements.runnable.FlowablePipelineElement;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


public abstract class BasePipelineElement<E extends PipelineElement> implements PipelineElement<E> {

	private final ElementEventHandler eventHandler;

	private volatile boolean valvesOpened;
	private E nextElement = null;

	private final BlockingQueue<Statement> sourcePipePort;
	private BlockingQueue<Statement> sinkPipePort;

	public BasePipelineElement(int sourceCapacity) {

		this(new ArrayBlockingQueue<>(sourceCapacity));
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
		nextElement.setSinkChannel(sourcePipePort);
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
	public BlockingQueue<Statement> getSinkChannel() {

		return sinkPipePort;
	}

	@Override
	public void setSinkChannel(BlockingQueue<Statement> queue) {

		this.sinkPipePort = queue;
	}

	@Override
	public BlockingQueue<Statement> getSourceChannel() {

		return sourcePipePort;
	}

	@Override
	public ElementEventHandler createEventHandler() throws FileNotFoundException {

		//return new ElementEventHandler.Mute();
		return new ElementLog(getName());
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

		if (nextElement != null) {
			nextElement.openValves(collector);
		}
	}

	@Override
	public void closeValves() throws IOException {

		valvesOpened = false;

		eventHandler.valvesClosed();

		if (sinkPipePort != null) {

			sinkPipePort.clear(); // drained
			eventHandler.sinkUnpiped();
		}
		if (sourcePipePort != null) {

			sourcePipePort.clear(); // drained
			eventHandler.sourceUnpiped();
		}

		eventHandler.disconnected();
	}

	public static class ElementLog extends BaseLog implements ElementEventHandler {

		public ElementLog(String threadName) throws FileNotFoundException {

			super(threadName);
		}

		@Override
		public void valvesOpened() {

			sendMessage("valves opened");
		}

		@Override
		public void sinkUnpiped() {

			sendMessage("sink disconnected");
		}

		@Override
		public void sourceUnpiped() {

			sendMessage("source disconnected");
		}

		@Override
		public void valvesClosed() {

			sendMessage("valves closed");
		}

		@Override
		public void disconnected() {

			sendMessage("disconnected");
		}
	}
}
