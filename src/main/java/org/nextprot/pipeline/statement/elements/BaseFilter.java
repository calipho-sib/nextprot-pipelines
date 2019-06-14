package org.nextprot.pipeline.statement.elements;



import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.Filter;
import org.nextprot.pipeline.statement.elements.runnable.BaseRunnablePipelineElement;
import org.nextprot.pipeline.statement.elements.runnable.FlowEventHandler;
import org.nextprot.pipeline.statement.muxdemux.DuplicableElement;

import java.io.FileNotFoundException;


public abstract class BaseFilter extends BasePipelineElement<DuplicableElement> implements DuplicableElement {

	protected BaseFilter(int capacity) {

		super(capacity);
	}

	public static abstract class RunnableFilter<F extends BaseFilter> extends BaseRunnablePipelineElement<F> implements Filter {

		public RunnableFilter(F pipelineElement) {
			super(pipelineElement);
		}

		@Override
		public FlowEventHandler createEventHandler() throws FileNotFoundException {

			return new FlowLog(getThreadName());
		}

		@Override
		public boolean handleFlow() throws Exception {

			return filter(pipelineElement.getSinkPipePort(), pipelineElement.getSourcePipePort());
		}
	}

	private static class FlowLog extends BaseLog implements FlowEventHandler {

		public FlowLog(String threadName) throws FileNotFoundException {

			super(threadName, "logs");
		}

		@Override
		public void elementOpened() {

			sendMessage("filter started");
		}

		@Override
		public void statementHandled(Statement statement) {

			sendMessage("filter statement "+ statement.getStatementId());
		}

		@Override
		public void endOfFlow() {

			sendMessage("end of flow");
		}
	}
}
