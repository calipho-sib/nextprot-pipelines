package org.nextprot.pipeline.statement.elements;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.elements.runnable.BaseRunnablePipelineElement;
import org.nextprot.pipeline.statement.elements.runnable.FlowEventHandler;

import java.io.FileNotFoundException;
import java.util.List;

public class NxFlatTableSink extends Sink {

	public enum Table {
		raw_statements,
		entry_mapped_statements
	}

	private final Table table;

	public NxFlatTableSink(Table table) {
		super();

		this.table = table;
	}

	@Override
	public Runnable newRunnableElement() {

		return new Runnable(this);
	}

	@Override
	public NxFlatTableSink duplicate(int capacity) {

		return new NxFlatTableSink(table);
	}

	private static class Runnable extends BaseRunnablePipelineElement<NxFlatTableSink> {

		private Runnable(NxFlatTableSink sink) {
			super(sink);
		}

		@Override
		public boolean handleFlow() throws Exception {

			//FlowEventHandler eh = flowEventHandlerHolder.get();

			Statement statement = getPipelineElement().getSinkPipePort().take();
			//eh.statementHandled(statement);

			return statement == END_OF_FLOW_TOKEN;
		}

		@Override
		public FlowEventHandler createEventHandler() throws FileNotFoundException {

			return new FlowLog(getThreadName(), getPipelineElement().table);
		}
	}

	private static class FlowLog extends BaseLog implements FlowEventHandler {

		private final Table table;

		public FlowLog(String threadName, Table table) throws FileNotFoundException {

			super(threadName);
			this.table = table;
		}

		@Override
		public void beginOfFlow() {

			sendMessage("opened");
		}

		@Override
		public void statementHandled(Statement statement) {

			sendMessage("write statement " + statement.getStatementId() + " in table "+ table);
		}

		@Override
		public void endOfFlow() {

			sendMessage("i statements evacuated");
		}
	}
}