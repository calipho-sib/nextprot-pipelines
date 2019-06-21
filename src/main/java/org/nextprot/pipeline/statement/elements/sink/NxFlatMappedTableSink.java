package org.nextprot.pipeline.statement.elements.sink;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.elements.Sink;
import org.nextprot.pipeline.statement.elements.NxFlatTable;
import org.nextprot.pipeline.statement.elements.flowable.BaseFlowLog;
import org.nextprot.pipeline.statement.elements.flowable.BaseFlowablePipelineElement;
import org.nextprot.pipeline.statement.elements.flowable.FlowEventHandler;

import java.io.FileNotFoundException;

import static org.nextprot.pipeline.statement.elements.Source.POISONED_STATEMENT;


public class NxFlatMappedTableSink extends Sink {

	private final NxFlatTable table = NxFlatTable.entry_mapped_statements;

	public NxFlatMappedTableSink() {

		super();
	}

	@Override
	public Flowable newFlowable() {

		return new Flowable(this);
	}

	@Override
	public NxFlatMappedTableSink duplicate(int newCapacity) {

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

			Statement statement = sink.getSinkChannel().take();
			getFlowEventHandler().statementHandled(statement);

			return statement == POISONED_STATEMENT;
		}

		@Override
		protected FlowEventHandler createFlowEventHandler() throws FileNotFoundException {

			return new FlowLog(getThreadName(), table);
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
}