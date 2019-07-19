package org.nextprot.pipeline.statement.core.stage.filter;



import org.nextprot.pipeline.statement.core.stage.BaseStage;
import org.nextprot.pipeline.statement.core.stage.DuplicableStage;


public abstract class BaseFilter extends BaseStage<DuplicableStage> implements DuplicableStage {

	protected BaseFilter(int capacity) {

		super(capacity);
	}
}
