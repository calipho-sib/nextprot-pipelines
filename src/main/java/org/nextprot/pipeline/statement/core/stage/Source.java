package org.nextprot.pipeline.statement.core.stage;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.core.Stage;
import org.nextprot.pipeline.statement.core.stage.handler.BaseFlowLog;

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

		throw new IllegalAccessError("Cannot set a pipeline element into a SOURCE stage");
	}

	@Override
	public BlockingQueue<Statement> getSinkChannel() {

		throw new IllegalAccessError("It is a Source element, can't connect to a Stage through this channel!");
	}

	protected int countPoisonedPillsToProduce() {

		Stage stage = this;

		while ((stage = stage.getFirstPipedStage()) != null) {

			int count = stage.countPipedStages();

			// one demux was found
			if (count > 1) {
				return count;
			}
		}
		return 1;
	}

	protected abstract Statement extract() throws IOException;

	protected abstract int extractionCapacity();

	@Override
	protected FlowLog createFlowEventHandler() throws FileNotFoundException {

		return new FlowLog(Thread.currentThread().getName(), extractionCapacity());
	}

	@Override
	public boolean handleFlow() throws Exception {

		FlowLog log = (FlowLog) getFlowEventHandler();

		Statement statement = extract();

		int pills = countPoisonedPillsToProduce();

		if (statement == null) {
			poisonChannel(pills);
			log.poisonedStatementReleased(pills, getSourceChannel());
			return true;
		}
		else {
			getSourceChannel().put(statement);
			log.statementHandled(statement, getSourceChannel());
		}

		return false;
	}

	private void poisonChannel(int pillsToProduce) throws InterruptedException {

		for (int i=0 ; i<pillsToProduce ; i++) {

			getSourceChannel().put(POISONED_STATEMENT);
		}
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
