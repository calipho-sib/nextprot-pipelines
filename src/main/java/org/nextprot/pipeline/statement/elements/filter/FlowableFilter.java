package org.nextprot.pipeline.statement.elements.filter;

import org.nextprot.pipeline.statement.elements.Filter;
import org.nextprot.pipeline.statement.elements.flowable.BaseFlowablePipelineElement;

abstract class FlowableFilter<F extends BaseFilter> extends BaseFlowablePipelineElement<F> implements Filter {

	FlowableFilter(F pipelineElement) {
		super(pipelineElement);
	}

	@Override
	public boolean handleFlow(F filter) throws Exception {

		return filter(filter.getSinkChannel(), filter.getSourceChannel());
	}
}
