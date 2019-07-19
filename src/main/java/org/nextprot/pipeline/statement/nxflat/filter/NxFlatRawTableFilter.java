package org.nextprot.pipeline.statement.nxflat.filter;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.core.stage.filter.BaseFilter;
import org.nextprot.pipeline.statement.core.stage.filter.FilterRunnableStage;
import org.nextprot.pipeline.statement.nxflat.NxFlatTable;
import org.nextprot.pipeline.statement.core.stage.flowable.BaseFlowLog;
import org.nextprot.pipeline.statement.core.stage.flowable.FlowEventHandler;

import java.io.FileNotFoundException;
import java.util.concurrent.BlockingQueue;

import static org.nextprot.pipeline.statement.core.stage.Source.POISONED_STATEMENT;

public class NxFlatRawTableFilter extends BaseFilter {

	private final NxFlatTable table = NxFlatTable.raw_statements;

	public NxFlatRawTableFilter(int capacity) {

		super(capacity);
	}

	@Override
	public RunnableStage newRunnableStage() {

		return new RunnableStage(this);
	}

	@Override
	public NxFlatRawTableFilter duplicate(int newCapacity) {

		return new NxFlatRawTableFilter(newCapacity);
	}

	private static class RunnableStage extends FilterRunnableStage<NxFlatRawTableFilter> {

		private final NxFlatTable table;

		private RunnableStage(NxFlatRawTableFilter filter) {
			super(filter);
			this.table = filter.table;
		}

		@Override
		protected FlowEventHandler createFlowEventHandler() throws Exception {

			return new FlowLog(Thread.currentThread().getName(), table);
		}

		@Override
		public boolean filter(BlockingQueue<Statement> in, BlockingQueue<Statement> out) throws Exception {

			Statement current = in.take();

			((FlowLog)getFlowEventHandler()).statementHandled(current, in, out);

			out.put(current);

			return current == POISONED_STATEMENT;
		}

		private static class FlowLog extends BaseFlowLog {

			private final NxFlatTable table;

			private FlowLog(String threadName, NxFlatTable table) throws FileNotFoundException {

				super(threadName);
				this.table = table;
			}

			public void beginOfFlow() {

				sendMessage("opened");
			}

			private void statementHandled(Statement statement, BlockingQueue<Statement> sinkChannel,
			                              BlockingQueue<Statement> sourceChannel) {

				statementHandled("load and transmit", statement, sinkChannel, sourceChannel);
			}

			@Override
			public void endOfFlow() {

				sendMessage(getStatementCount()+" healthy statements loaded in table "+ table + " and passed to next filter");
			}
		}
	}
}