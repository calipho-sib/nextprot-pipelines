package org.nextprot.pipeline.statement.core.elements.filter;



import org.nextprot.pipeline.statement.core.elements.BaseStage;
import org.nextprot.pipeline.statement.core.elements.demux.DuplicableStage;


public abstract class BaseFilter extends BaseStage<DuplicableStage> implements DuplicableStage {

	protected BaseFilter(int capacity) {

		super(capacity);
	}
}
