package org.nextprot.pipeline.statement.core.elements.filter;

import org.nextprot.pipeline.statement.core.elements.Filter;
import org.nextprot.pipeline.statement.core.elements.flowable.BaseFlowablePipelineElement;

public abstract class FlowableFilter<F extends BaseFilter> extends BaseFlowablePipelineElement<F> implements Filter {

	protected FlowableFilter(F pipelineElement) {
		super(pipelineElement);
	}

	@Override
	public boolean handleFlow(F filter) throws Exception {

		return filter(filter.getSinkChannel(), filter.getSourceChannel());
	}
}
