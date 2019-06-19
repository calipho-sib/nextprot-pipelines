package org.nextprot.pipeline.statement.elements;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.NxFlatTable;
import org.nextprot.pipeline.statement.elements.runnable.BaseFlowLog;
import org.nextprot.pipeline.statement.elements.runnable.FlowEventHandler;

import java.io.FileNotFoundException;
import java.util.concurrent.BlockingQueue;

import static org.nextprot.pipeline.statement.elements.runnable.BaseFlowablePipelineElement.END_OF_FLOW_STATEMENT;

public class NxFlatRawTableFilter extends BaseFilter {

	private final NxFlatTable table;

	public NxFlatRawTableFilter(int capacity) {
		super(capacity);

		this.table = NxFlatTable.raw_statements;
	}

	@Override
	public Flowable newFlowable() {

		return new Flowable(this);
	}

	@Override
	public NxFlatRawTableFilter duplicate(int capacity) {

		return new NxFlatRawTableFilter(capacity);
	}

	private static class Flowable extends FlowableFilter<NxFlatRawTableFilter> {

		private final NxFlatTable table;

		private Flowable(NxFlatRawTableFilter filter) {
			super(filter);
			this.table = filter.table;
		}

		@Override
		public FlowEventHandler createEventHandler() throws FileNotFoundException {

			return new FlowLog(getThreadName(), table);
		}

		@Override
		public boolean filter(BlockingQueue<Statement> in, BlockingQueue<Statement> out) throws Exception {

			Statement current = in.take();

			load(current);

			out.put(current);

			return current == END_OF_FLOW_STATEMENT;
		}

		private void load(Statement statement) {

			flowEventHandlerHolder.get().statementHandled(statement);
		}
	}

	private static class FlowLog extends BaseFlowLog {

		private final NxFlatTable table;

		public FlowLog(String threadName, NxFlatTable table) throws FileNotFoundException {

			super(threadName);
			this.table = table;
		}

		public void beginOfFlow() {

			sendMessage("opened");
		}

		@Override
		public void statementHandled(Statement statement) {

			super.statementHandled(statement);

			if (statement == END_OF_FLOW_STATEMENT) {
				sendMessage(getStatementId(statement) + " transmitted to next filter");
			}
			else {
				sendMessage("load statement " + statement.getStatementId() + " and passed to next filter");
			}
		}

		@Override
		public void endOfFlow() {

			sendMessage(getStatementCount()+" statements loaded in table "+ table + " and passed to next filter");
		}
	}
}