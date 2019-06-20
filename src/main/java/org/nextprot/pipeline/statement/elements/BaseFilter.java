package org.nextprot.pipeline.statement.elements;



import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.Filter;
import org.nextprot.pipeline.statement.elements.runnable.BaseFlowLog;
import org.nextprot.pipeline.statement.elements.runnable.BaseFlowablePipelineElement;
import org.nextprot.pipeline.statement.elements.runnable.FlowEventHandler;
import org.nextprot.pipeline.statement.muxdemux.DuplicableElement;

import java.io.FileNotFoundException;
import java.util.concurrent.BlockingQueue;


public abstract class BaseFilter extends BasePipelineElement<DuplicableElement> implements DuplicableElement {

	protected BaseFilter(int capacity) {

		super(capacity);
	}

	public static abstract class FlowableFilter<F extends BaseFilter> extends BaseFlowablePipelineElement<F> implements Filter {

		public FlowableFilter(F pipelineElement) {
			super(pipelineElement);
		}

		@Override
		public FlowEventHandler createEventHandler() throws FileNotFoundException {

			return new FlowLog(getThreadName());
		}

		@Override
		public boolean handleFlow(F filter) throws Exception {

			return filter(filter.getSinkChannel(), filter.getSourceChannel());
		}
	}

	public static class FlowLog extends BaseFlowLog {

		public FlowLog(String threadName) throws FileNotFoundException {

			super(threadName);
		}

		@Override
		public void beginOfFlow() {

			sendMessage("start filtering flow");
		}

		protected void statementHandled(Statement statement, BlockingQueue<Statement> sinkChannel,
		                              BlockingQueue<Statement> sourceChannel) {

			statementHandled("filtering", statement, sinkChannel, sourceChannel);
		}

		@Override
		public void endOfFlow() {

			sendMessage(getStatementCount()+ " filtered");
		}
	}
}
