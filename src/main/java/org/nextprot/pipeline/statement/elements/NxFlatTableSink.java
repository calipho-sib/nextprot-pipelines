package org.nextprot.pipeline.statement.elements;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.elements.runnable.AbstractRunnablePipelineElement;

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
	public RunnableNxFlatTableSink newRunnableElement() {

		return new RunnableNxFlatTableSink<>(this);
	}

	@Override
	public NxFlatTableSink duplicate(int capacity) {

		return new NxFlatTableSink(table);
	}

	private static class RunnableNxFlatTableSink<S extends Sink> extends AbstractRunnablePipelineElement<S> {

		private RunnableNxFlatTableSink(S sink) {
			super(sink.getSinkPipePort().capacity(), sink);
		}

		@Override
		public boolean handleFlow(List<Statement> buffer) throws IOException {

			Statement statement = pipelineElement.getSinkPipePort().read();
			statementsHandled(1);
			return statement == END_OF_FLOW_TOKEN;
		}

		@Override
		public void endOfFlow() {
			//printlnTextInLog(i + " statements evacuated");
		}

		@Override
		public void statementsHandled(int statements) {
			//printlnTextInLog("write statement " + statement.getStatementId() + " in table " + table);
		}
	}
}