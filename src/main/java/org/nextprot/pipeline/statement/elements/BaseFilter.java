package org.nextprot.pipeline.statement.elements;



import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.Filter;
import org.nextprot.pipeline.statement.elements.runnable.AbstractRunnablePipelineElement;
import org.nextprot.pipeline.statement.muxdemux.DuplicableElement;
import org.nextprot.pipeline.statement.ports.SinkPipePort;
import org.nextprot.pipeline.statement.ports.SourcePipePort;

import java.io.IOException;
import java.util.List;


public abstract class BaseFilter extends BasePipelineElement<DuplicableElement> implements DuplicableElement {

	protected BaseFilter(int capacity) {

		super(new SinkPipePort(capacity), new SourcePipePort(capacity));
	}

	public static abstract class RunnableFilter<F extends BaseFilter> extends AbstractRunnablePipelineElement<F> implements Filter {

		public RunnableFilter(F pipelineElement) {
			super(pipelineElement.getSinkPipePort().capacity(), pipelineElement);
		}

		@Override
		public boolean handleFlow(List<Statement> buffer) throws IOException {

			return filter(pipelineElement.getSinkPipePort(), pipelineElement.getSourcePipePort());
		}
	}
}
