package org.nextprot.pipeline.statement.elements;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.elements.runnable.BaseRunnablePipelineElement;
import org.nextprot.pipeline.statement.elements.runnable.FlowEventHandler;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

public class NxFlatTableSink extends Sink {

	public enum Table {
		raw_statements,
		entry_mapped_statements
	}

	private final Table table;

	public NxFlatTableSink(Table table) {
		super(1);

		this.table = table;
	}

	@Override
	public Runnable newRunnableElement() {

		return new Runnable<>(this);
	}

	@Override
	public NxFlatTableSink duplicate(int capacity) {

		return new NxFlatTableSink(table);
	}

	/*@Override
	public ElementEventHandler getEventHandler() {

		return new ElementLog("NxFlatTableSink-main");
	}*/

	private static class Runnable<S extends Sink> extends BaseRunnablePipelineElement<S> {

		private Runnable(S sink) {
			super(sink.getSinkPipePort().capacity(), sink);
		}

		@Override
		public boolean handleFlow(List<Statement> buffer) throws IOException {

			Statement statement = pipelineElement.getSinkPipePort().read();
			createEventHandler().statementsHandled(1);
			return statement == END_OF_FLOW_TOKEN;
		}

		@Override
		public FlowEventHandler createEventHandler() throws FileNotFoundException {

			return new FlowLog(getThreadName());
		}
	}

	private static class FlowLog extends BaseLog implements FlowEventHandler {

		public FlowLog(String threadName) throws FileNotFoundException {

			super(threadName, "logs");
		}

		@Override
		public void elementOpened(int capacity) {

			sendMessage("opened (capacity=" + capacity + ")");
		}

		@Override
		public void statementsHandled(int statements) {

			sendMessage("write statement  + statement.getStatementId() +  in table table");
		}

		@Override
		public void endOfFlow() {
			sendMessage("i statements evacuated");
		}
	}
}