package org.nextprot.pipeline.statement.nxflat.sink;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.core.stage.Sink;
import org.nextprot.pipeline.statement.nxflat.NxFlatTable;
import org.nextprot.pipeline.statement.core.stage.handler.BaseFlowLog;
import org.nextprot.pipeline.statement.core.stage.handler.FlowEventHandler;

import java.io.FileNotFoundException;

import static org.nextprot.pipeline.statement.core.stage.Source.POISONED_STATEMENT;


public class NxFlatMappedTableSink extends Sink {

	private final NxFlatTable table = NxFlatTable.entry_mapped_statements;

	public NxFlatMappedTableSink() {

		super();
	}

	@Override
	public NxFlatMappedTableSink duplicate(int newCapacity) {

		return new NxFlatMappedTableSink();
	}
	@Override
	public boolean handleFlow() throws Exception {

		Statement statement = getSinkChannel().take();
		getFlowEventHandler().statementHandled(statement);

		return statement == POISONED_STATEMENT;
	}

	@Override
	protected FlowEventHandler createFlowEventHandler() throws FileNotFoundException {

		return new FlowLog(Thread.currentThread().getName(), table);
	}

	private static class FlowLog extends BaseFlowLog {

		private final NxFlatTable table;

		private FlowLog(String threadName, NxFlatTable table) throws FileNotFoundException {

			super(threadName);
			this.table = table;
		}

		@Override
		public void beginOfFlow() {

			sendMessage("opened");
		}

		@Override
		public void statementHandled(Statement statement) {

			super.statementHandled(statement);

			if (statement != POISONED_STATEMENT) {
				sendMessage("load statement " + statement.getStatementId());
			}
		}

		@Override
		public void endOfFlow() {

			sendMessage(getStatementCount()+" statements loaded in table "+ table);
		}
	}
}