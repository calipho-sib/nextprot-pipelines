package org.nextprot.pipeline.statement.elements.source;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.elements.Source;
import org.nextprot.pipeline.statement.elements.flowable.BaseFlowLog;
import org.nextprot.pipeline.statement.elements.flowable.BaseFlowablePipelineElement;
import org.nextprot.pipeline.statement.elements.flowable.FlowEventHandler;

import java.io.FileNotFoundException;
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

	private synchronized Statement pump() throws IOException {

		return pump.pump();
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

	private static class Flowable extends BaseFlowablePipelineElement<PumpBasedSource> {

		private final int capacity;
		private final int pills;

		private Flowable(PumpBasedSource source, int capacity, int pills) {

			super(source);
			this.capacity = capacity;
			this.pills = pills;
		}

		@Override
		public boolean handleFlow(PumpBasedSource source) throws Exception {

			FlowLog log = (FlowLog) getFlowEventHandler();

			Statement statement = source.pump();

			if (statement == null) {
				poisonChannel(source.getSourceChannel());
				log.poisonedStatementReleased(pills, source.getSourceChannel());
				return true;
			}
			else {
				source.getSourceChannel().put(statement);
				log.statementHandled(statement, source.getSourceChannel());
			}

			return false;
		}

		private void poisonChannel(BlockingQueue<Statement> sourceChannel) throws InterruptedException {

			for (int i=0 ; i<pills ; i++) {

				sourceChannel.put(POISONED_STATEMENT);
			}
		}

		@Override
		protected FlowEventHandler createFlowEventHandler() throws FileNotFoundException {

			return new FlowLog(getThreadName(), capacity);
		}

		private static class FlowLog extends BaseFlowLog {

			private final int capacity;

			private FlowLog(String threadName, int capacity) throws FileNotFoundException {

				super(threadName);
				this.capacity = capacity;
			}

			@Override
			public void beginOfFlow() {

				sendMessage("pump started (capacity="+ capacity + ")");
			}

			private void statementHandled(Statement statement, BlockingQueue<Statement> sourceChannel) {

				statementHandled("pump", statement, null, sourceChannel);
			}

			private void poisonedStatementReleased(int pills, BlockingQueue<Statement> sourceChannel) {

				sendMessage(pills+" poisoned statement"+(pills>1 ? "s":"")+" released into the source channel #" + sourceChannel.hashCode());
			}

			@Override
			public void endOfFlow() {

				sendMessage(getStatementCount()+" statements pumped");
			}
		}
	}
}
