package org.nextprot.pipeline.statement.core.stage;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.core.Stage;
import org.nextprot.pipeline.statement.core.stage.runnable.BaseFlowLog;
import org.nextprot.pipeline.statement.core.stage.runnable.BaseRunnableStage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

/**
 * A Source is the first pipeline element - it cannot be piped to another element
 *
 *    -----    -----
 *   |  S  >==<  F  >== ...
 *    -----    -----
 */
public abstract class Source extends BaseStage<Stage> {

	/** poisoned statement pill */
	public static final Statement POISONED_STATEMENT = new Statement();

	protected Source(int sourceCapacity) {

		super(sourceCapacity);
	}

	@Override
	public void setSinkChannel(BlockingQueue<Statement> sinkChannel) {

		throw new Error("Cannot set a pipeline element into a SOURCE");
	}

	@Override
	public BlockingQueue<Statement> getSinkChannel() {

		throw new Error("It is a SOURCE element, cannot pipe to a pipeline element through this channel!");
	}

	protected int countPoisonedPillsToProduce() {

		Stage element = this;

		// look for the max next stage number
		while ((element = element.getFirstPipedStage()) != null) {

			int count = element.countPipedStages();

			if (count > 1) {
				return count;
			}
		}
		return 1;
	}

	@Override
	public RunnableStage newRunnableStage() {

		return new RunnableStage(this, extractionCapacity(), countPoisonedPillsToProduce());
	}

	protected abstract Statement extract() throws IOException;

	protected abstract int extractionCapacity();

	protected static class RunnableStage extends BaseRunnableStage<Source> {

		private final int capacity;
		private final int pills;

		public RunnableStage(Source source, int capacity, int pills) {

			super(source);
			this.capacity = capacity;
			this.pills = pills;
		}

		@Override
		public boolean handleFlow() throws Exception {

			FlowLog log = (FlowLog) getFlowEventHandler();

			Source source = getStage();
			Statement statement = source.extract();

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
		protected FlowLog createFlowEventHandler() throws FileNotFoundException {

			return new FlowLog(Thread.currentThread().getName(), capacity);
		}

		private static class FlowLog extends BaseFlowLog {

			private final int capacity;

			private FlowLog(String threadName, int capacity) throws FileNotFoundException {

				super(threadName);
				this.capacity = capacity;
			}

			@Override
			public void beginOfFlow() {

				sendMessage("statement extraction started (capacity="+ capacity + ")");
			}

			private void statementHandled(Statement statement, BlockingQueue<Statement> sourceChannel) {

				statementHandled("extract", statement, null, sourceChannel);
			}

			private void poisonedStatementReleased(int pills, BlockingQueue<Statement> sourceChannel) {

				sendMessage(pills+" poisoned statement"+(pills>1 ? "s":"")+" released into the source channel #" + sourceChannel.hashCode());
			}

			@Override
			public void endOfFlow() {

				sendMessage(getStatementCount()+" statements extracted");
			}
		}
	}
}
