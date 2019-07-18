package org.nextprot.pipeline.statement.core.elements.filter;

import org.nextprot.pipeline.statement.core.elements.Filter;
import org.nextprot.pipeline.statement.core.elements.flowable.BaseRunnableStage;

public abstract class FilterRunnableStage<F extends BaseFilter> extends BaseRunnableStage<F> implements Filter {

	protected FilterRunnableStage(F pipelineElement) {
		super(pipelineElement);
	}

	@Override
	public boolean handleFlow() throws Exception {

		return filter(getStage().getSinkChannel(), getStage().getSourceChannel());
	}
}
