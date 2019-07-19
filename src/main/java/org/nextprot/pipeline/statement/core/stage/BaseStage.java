package org.nextprot.pipeline.statement.core.stage;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.core.BaseLog;
import org.nextprot.pipeline.statement.core.Stage;

import java.io.FileNotFoundException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;


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
public abstract class BaseStage<E extends Stage> implements Stage<E> {

	private E nextStage = null;

	private final BlockingQueue<Statement> sourceChannel;
	private BlockingQueue<Statement> sinkChannel;
	private final ElementEventHandler eventHandler;

	public BaseStage(int sourceCapacity) {

		this(new ArrayBlockingQueue<>(sourceCapacity));
	}

	public BaseStage(BlockingQueue<Statement> sourceChannel) {

		this.sourceChannel = sourceChannel;

		try {
			eventHandler = createElementEventHandler();
		} catch (FileNotFoundException e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public void pipe(E nextStage) {

		this.nextStage = nextStage;
		nextStage.setSinkChannel(sourceChannel);
	}

	@Override
	public String getName() {

		return getClass().getSimpleName();
	}

	@Override
	public Stream<E> nextStages() {

		return Stream.of(nextStage);
	}

	@Override
	public E nextStage() {

		return nextStage;
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
	public void close() {

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
	}
}
