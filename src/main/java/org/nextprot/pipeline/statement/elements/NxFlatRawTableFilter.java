package org.nextprot.pipeline.statement.elements;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.NxFlatTable;
import org.nextprot.pipeline.statement.elements.runnable.BaseFlowLog;
import org.nextprot.pipeline.statement.elements.runnable.FlowEventHandler;

import java.io.FileNotFoundException;
import java.util.concurrent.BlockingQueue;

public class NxFlatRawTableFilter extends BaseFilter {

	private final NxFlatTable table = NxFlatTable.raw_statements;

	public NxFlatRawTableFilter(int capacity) {

		super(capacity);
	}

	@Override
	public Flowable newFlowable() {

		return new Flowable(this);
	}

	@Override
	public NxFlatRawTableFilter duplicate(int newCapacity) {

		return new NxFlatRawTableFilter(newCapacity);
	}

	private static class Flowable extends FlowableFilter<NxFlatRawTableFilter> {

		private final NxFlatTable table;

		private Flowable(NxFlatRawTableFilter filter) {
			super(filter);
			this.table = filter.table;
		}

		@Override
		protected FlowEventHandler createFlowEventHandler() throws Exception {

			return new FlowLog(getThreadName(), table);
		}

		@Override
		public boolean filter(BlockingQueue<Statement> in, BlockingQueue<Statement> out) throws Exception {

			Statement current = in.take();

			((FlowLog)getFlowEventHandler()).statementHandled(current, in, out);

			out.put(current);

			return current == POISONED_STATEMENT;
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