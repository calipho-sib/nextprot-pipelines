package org.nextprot.pipeline.statement.core.elements.source;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.core.elements.Source;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

/**
 * This class is a source of data for a pipe of threads.
 * It pumps statements and send them in a connected receiver
 * but cannot serve as a receiver for any other Pipe: it must always be at the beginning,
 * or "source" of the pipe.
 **/
public class PumpBasedSource extends Source {

	private final Pump<Statement> pump;

	public PumpBasedSource(Pump<Statement> pump) {

		super(pump.capacity());
		this.pump = pump;
	}

	@Override
	protected synchronized Statement mine() throws IOException {

		return pump.pump();
	}

	@Override
	protected int extractionCapacity() {

		return pump.capacity();
	}

	@Override
	public synchronized void closeValves() throws IOException {

		pump.stop();
		super.closeValves();
	}

	@Override
	public BlockingQueue<Statement> getSinkChannel() {

		throw new Error("It is a Source element, can't connect to a PipelineElement through this channel!");
	}

	@Override
	public Flowable newFlowable() {

		return new Flowable(this, pump.capacity(), countPoisonedPillsToProduce());
	}
}
