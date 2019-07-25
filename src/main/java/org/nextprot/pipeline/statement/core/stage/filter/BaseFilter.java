package org.nextprot.pipeline.statement.core.stage.filter;



import org.nextprot.pipeline.statement.core.stage.BaseStage;
import org.nextprot.pipeline.statement.core.stage.DuplicableStage;
import org.nextprot.pipeline.statement.core.stage.Filter;


public abstract class BaseFilter extends BaseStage<DuplicableStage> implements Filter, DuplicableStage {

	protected BaseFilter(int capacity) {

		super(capacity);
	}

	@Override
	public boolean handleFlow() throws Exception {

		return filter(getSinkChannel(), getSourceChannel());
	}
}
