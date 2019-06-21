package org.nextprot.pipeline.statement.elements.demux;


import org.nextprot.pipeline.statement.PipelineElement;


public interface DuplicableElement extends PipelineElement<DuplicableElement> {

	/**
	 * Duplicate this element with a new capacity
	 * @param newCapacity the new capacity
	 * @return a copy of itself
	 */
	DuplicableElement duplicate(int newCapacity);
}
