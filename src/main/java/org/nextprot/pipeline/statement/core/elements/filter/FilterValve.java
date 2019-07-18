package org.nextprot.pipeline.statement.core.elements.filter;

import org.nextprot.pipeline.statement.core.elements.Filter;
import org.nextprot.pipeline.statement.core.elements.flowable.BaseValve;

public abstract class FilterValve<F extends BaseFilter> extends BaseValve<F> implements Filter {

	protected FilterValve(F pipelineElement) {
		super(pipelineElement);
	}

	@Override
	public boolean handleFlow() throws Exception {

		return filter(getStage().getSinkChannel(), getStage().getSourceChannel());
	}
}
