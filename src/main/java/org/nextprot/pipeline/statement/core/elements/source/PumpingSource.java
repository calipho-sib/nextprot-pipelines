package org.nextprot.pipeline.statement.core.elements.source;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.core.elements.Source;

import java.util.concurrent.BlockingQueue;

/**
 * This class is a source of data for a pipe of threads.
 * It pumps statements and send them in a connected receiver
 * but cannot serve as a receiver for any other Pipe: it must always be at the beginning,
 * or "source" of the pipe.
 **/
public class PumpingSource extends Source {

	private final Pump<Statement> pump;
	private final int capacity;

	public PumpingSource(Pump<Statement> pump, int capacity) {

		super(capacity);
		this.pump = pump;
		this.capacity = capacity;
	}

	@Override
	protected synchronized Statement extract() {

		return pump.pump();
	}

	@Override
	protected int extractionCapacity() {

		return capacity;
	}

	@Override
	public synchronized void closeValves() {

		pump.stop();
		super.closeValves();
	}

	@Override
	public BlockingQueue<Statement> getSinkChannel() {

		throw new Error("It is a Source element, can't connect to a PipelineElement through this channel!");
	}

	@Override
	public Valve newValve() {

		return new Valve(this, 1, countPoisonedPillsToProduce());
	}
}
