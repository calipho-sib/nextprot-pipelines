package org.nextprot.pipeline.statement.elements;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.NxFlatTable;
import org.nextprot.pipeline.statement.elements.runnable.BaseFlowablePipelineElement;
import org.nextprot.pipeline.statement.elements.runnable.FlowEventHandler;

import java.io.FileNotFoundException;

import static org.nextprot.pipeline.statement.elements.runnable.BaseFlowablePipelineElement.END_OF_FLOW_TOKEN;

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

		private Flowable(NxFlatMappedTableSink sink) {
			super(sink);
		}

		@Override
		public boolean handleFlow() throws Exception {

			FlowEventHandler eh = flowEventHandlerHolder.get();

			Statement statement = getPipelineElement().getSinkPipePort().take();
			eh.statementHandled(statement);

			return statement == END_OF_FLOW_TOKEN;
		}

		@Override
		public FlowEventHandler createEventHandler() throws FileNotFoundException {

			return new FlowLog(getThreadName(), getPipelineElement().table);
		}
	}

	private static class FlowLog extends BaseLog implements FlowEventHandler {

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

			if (statement != END_OF_FLOW_TOKEN) {
				sendMessage("load statement " + statement.getStatementId() + " in table "+ table);
			}
		}

		@Override
		public void endOfFlow() {

			sendMessage("i statements loaded");
		}
	}
}