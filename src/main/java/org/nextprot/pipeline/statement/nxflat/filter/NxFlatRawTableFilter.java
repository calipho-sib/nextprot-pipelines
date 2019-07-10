package org.nextprot.pipeline.statement.nxflat.filter;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.core.elements.filter.BaseFilter;
import org.nextprot.pipeline.statement.core.elements.filter.FilterValve;
import org.nextprot.pipeline.statement.nxflat.NxFlatTable;
import org.nextprot.pipeline.statement.core.elements.flowable.BaseFlowLog;
import org.nextprot.pipeline.statement.core.elements.flowable.FlowEventHandler;

import java.io.FileNotFoundException;
import java.util.concurrent.BlockingQueue;

import static org.nextprot.pipeline.statement.core.elements.Source.POISONED_STATEMENT;

public class NxFlatRawTableFilter extends BaseFilter {

	private final NxFlatTable table = NxFlatTable.raw_statements;

	public NxFlatRawTableFilter(int capacity) {

		super(capacity);
	}

	@Override
	public Valve newValve() {

		return new Valve(this);
	}

	@Override
	public NxFlatRawTableFilter duplicate(int newCapacity) {

		return new NxFlatRawTableFilter(newCapacity);
	}

	private static class Valve extends FilterValve<NxFlatRawTableFilter> {

		private final NxFlatTable table;

		private Valve(NxFlatRawTableFilter filter) {
			super(filter);
			this.table = filter.table;
		}

		@Override
		protected FlowEventHandler createFlowEventHandler() throws Exception {

			return new FlowLog(getName(), table);
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