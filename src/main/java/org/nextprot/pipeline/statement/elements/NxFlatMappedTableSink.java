package org.nextprot.pipeline.statement.elements;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.NxFlatTable;
import org.nextprot.pipeline.statement.elements.runnable.BaseFlowLog;
import org.nextprot.pipeline.statement.elements.runnable.BaseFlowablePipelineElement;
import org.nextprot.pipeline.statement.elements.runnable.FlowEventHandler;

import java.io.FileNotFoundException;

import static org.nextprot.pipeline.statement.elements.runnable.BaseFlowablePipelineElement.END_OF_FLOW_STATEMENT;

public class NxFlatMappedTableSink extends Sink {

	private final NxFlatTable table;

	public NxFlatMappedTableSink() {
		super();

		this.table = NxFlatTable.entry_mapped_statements;
	}

	@Override
	public Flowable newFlowable() {

		return new Flowable(this);
	}

	@Override
	public NxFlatMappedTableSink duplicate(int capacity) {

		return new NxFlatMappedTableSink();
	}

	private static class Flowable extends BaseFlowablePipelineElement<NxFlatMappedTableSink> {

		private final NxFlatTable table;

		private Flowable(NxFlatMappedTableSink sink) {
			super(sink);

			this.table = sink.table;
		}

		@Override
		public boolean handleFlow(NxFlatMappedTableSink sink) throws Exception {

			FlowEventHandler eh = flowEventHandlerHolder.get();

			Statement statement = sink.getSinkPipePort().take();
			eh.statementHandled(statement);

			return statement == END_OF_FLOW_STATEMENT;
		}

		@Override
		public FlowEventHandler createEventHandler() throws FileNotFoundException {

			return new FlowLog(getThreadName(), table);
		}
	}

	private static class FlowLog extends BaseFlowLog {

		private final NxFlatTable table;

		public FlowLog(String threadName, NxFlatTable table) throws FileNotFoundException {

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

			if (statement != END_OF_FLOW_STATEMENT) {
				sendMessage("load statement " + statement.getStatementId());
			}
		}

		@Override
		public void endOfFlow() {

			sendMessage(getStatementCount()+" statements loaded in table "+ table);
		}
	}
}