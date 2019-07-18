package org.nextprot.pipeline.statement.core.elements;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.core.PipelineElement;
import org.nextprot.pipeline.statement.core.elements.flowable.BaseFlowLog;
import org.nextprot.pipeline.statement.core.elements.flowable.BaseValve;

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
public abstract class Source extends BasePipelineElement<PipelineElement> {

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

		PipelineElement element = this;

		int max = 0;

		// look for the max next stage number
		while ((element = element.nextStage()) != null) {

			int count = element.countStages();

			if (count > max) {
				max = count;
			}
		}
		return max;
	}

	@Override
	public Valve newValve() {

		return new Valve(this, extractionCapacity(), countPoisonedPillsToProduce());
	}

	protected abstract Statement extract() throws IOException;

	protected abstract int extractionCapacity();

	protected static class Valve extends BaseValve<Source> {

		private final int capacity;
		private final int pills;

		public Valve(Source source, int capacity, int pills) {

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
