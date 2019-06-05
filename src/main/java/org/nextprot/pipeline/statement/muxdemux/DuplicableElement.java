package org.nextprot.pipeline.statement.muxdemux;


import org.nextprot.pipeline.statement.PipelineElement;


public interface DuplicableElement extends PipelineElement<DuplicableElement> {

	/**
	 * Duplicate this element with a new capacity value
	 * @param newCapacity the new capacity
	 * @return a copy of itself
	 */
	DuplicableElement duplicate(int newCapacity);
}
