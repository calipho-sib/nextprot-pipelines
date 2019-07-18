package org.nextprot.pipeline.statement.core.elements;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.core.BaseLog;
import org.nextprot.pipeline.statement.core.PipelineElement;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


/**
 * A base class representing a pipeline element with
 * a source channel (==) and a pluggable sink (<) ready to connect
 *
 * 1. One unconnected element E1:
 *    -----
 *   < E1  >==
 *    -----
 * 2. Two connected elements (after E1.pipe(E2))
 *    -----    -----
 *   < E1  >==< E2  >==
 *    -----    -----
 *
 * @param <E>
 *
 * WARNING: Not thread safe, should run on a single thread !
 */
public abstract class BasePipelineElement<E extends PipelineElement> implements PipelineElement<E> {

	private E nextElement = null;

	private final BlockingQueue<Statement> sourceChannel;
	private BlockingQueue<Statement> sinkChannel;
	private final ElementEventHandler eventHandler;

	public BasePipelineElement(int sourceCapacity) {

		this(new ArrayBlockingQueue<>(sourceCapacity));
	}

	public BasePipelineElement(BlockingQueue<Statement> sourceChannel) {

		this.sourceChannel = sourceChannel;

		try {
			eventHandler = createElementEventHandler();
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
		nextElement.setSinkChannel(sourceChannel);
	}

	@Override
	public String getName() {

		return getClass().getSimpleName();
	}

	@Override
	public E nextSink() {

		return nextElement;
	}

	/**
	 * This protected method requests a Pipe threads to create and return
	 * a PipedInputPort thread so that another Pipe thread can connect to it.
	 **/
	@Override
	public BlockingQueue<Statement> getSinkChannel() {

		return sinkChannel;
	}

	@Override
	public void setSinkChannel(BlockingQueue<Statement> sinkChannel) {

		this.sinkChannel = sinkChannel;
		eventHandler.sinkPiped();
	}

	@Override
	public BlockingQueue<Statement> getSourceChannel() {

		return sourceChannel;
	}

	protected ElementEventHandler createElementEventHandler() throws FileNotFoundException {

		//return new ElementEventHandler.Mute();
		return new ElementLog(getName());
	}

	@Override
	public void openValves(List<Thread> runningValves) {

		Thread runningValve = newActiveValve();
		eventHandler.valvesOpened();

		runningValves.add(runningValve);

		if (nextElement != null) {
			nextElement.openValves(runningValves);
		}
	}

	@Override
	public void closeValves() {

		eventHandler.valvesClosed();

		if (sinkChannel != null) {

			eventHandler.sinkUnpiped();
		}
		if (sourceChannel != null) {

			eventHandler.sourceUnpiped();
		}
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
		public void sinkPiped() {

			sendMessage("sink channel port connected");
		}

		@Override
		public void sinkUnpiped() {

			sendMessage("sink channel port disconnected");
		}

		@Override
		public void sourceUnpiped() {

			sendMessage("source channel port disconnected");
		}

		@Override
		public void valvesClosed() {

			sendMessage("valves closed");
		}
	}
}
