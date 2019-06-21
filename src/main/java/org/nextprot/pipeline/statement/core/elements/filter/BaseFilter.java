package org.nextprot.pipeline.statement.core.elements.filter;



import org.nextprot.pipeline.statement.core.elements.BasePipelineElement;
import org.nextprot.pipeline.statement.core.elements.demux.DuplicableElement;


public abstract class BaseFilter extends BasePipelineElement<DuplicableElement> implements DuplicableElement {

	protected BaseFilter(int capacity) {

		super(capacity);
	}
}
