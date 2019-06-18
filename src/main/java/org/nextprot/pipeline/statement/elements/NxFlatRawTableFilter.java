package org.nextprot.pipeline.statement.elements;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.NxFlatTable;
import org.nextprot.pipeline.statement.elements.runnable.FlowEventHandler;

import java.io.FileNotFoundException;
import java.util.concurrent.BlockingQueue;

import static org.nextprot.pipeline.statement.elements.runnable.BaseFlowablePipelineElement.END_OF_FLOW_TOKEN;

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

		private Flowable(NxFlatRawTableFilter filter) {
			super(filter);
		}

		@Override
		public FlowEventHandler createEventHandler() throws FileNotFoundException {

			return new FlowLog(getThreadName(), getPipelineElement().table);
		}

		@Override
		public boolean filter(BlockingQueue<Statement> in, BlockingQueue<Statement> out) throws Exception {

			Statement current = in.take();

			load(current);

			out.put(current);

			return current == END_OF_FLOW_TOKEN;
		}

		private void load(Statement statement) {

			flowEventHandlerHolder.get().statementHandled(statement);
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

			if (statement == END_OF_FLOW_TOKEN) {
				sendMessage(getStatementId(statement) + " transmitted to next filter");
			}
			else {
				sendMessage("load statement " + statement.getStatementId() + " in table "+ table + " and transmitted to next filter");
			}
		}

		@Override
		public void endOfFlow() {

			sendMessage("i statements loaded");
		}
	}
}