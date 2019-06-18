package org.nextprot.pipeline.statement.elements;



import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.Filter;
import org.nextprot.pipeline.statement.elements.runnable.BaseFlowablePipelineElement;
import org.nextprot.pipeline.statement.elements.runnable.FlowEventHandler;
import org.nextprot.pipeline.statement.muxdemux.DuplicableElement;

import java.io.FileNotFoundException;


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
		public boolean handleFlow() throws Exception {

			F element = getPipelineElement();

			return filter(element.getSinkPipePort(), element.getSourcePipePort());
		}
	}

	private static class FlowLog extends BaseLog implements FlowEventHandler {

		public FlowLog(String threadName) throws FileNotFoundException {

			super(threadName);
		}

		@Override
		public void beginOfFlow() {

			sendMessage("start filtering flow");
		}

		@Override
		public void statementHandled(Statement statement) {

			sendMessage("filter statement "+ getStatementId(statement));
		}

		@Override
		public void endOfFlow() {

			sendMessage("end of flow");
		}
	}
}
