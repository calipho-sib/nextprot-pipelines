package org.nextprot.pipeline.statement.elements.filter;



import org.nextprot.pipeline.statement.elements.BasePipelineElement;
import org.nextprot.pipeline.statement.elements.demux.DuplicableElement;


abstract class BaseFilter extends BasePipelineElement<DuplicableElement> implements DuplicableElement {

	BaseFilter(int capacity) {

		super(capacity);
	}
}
