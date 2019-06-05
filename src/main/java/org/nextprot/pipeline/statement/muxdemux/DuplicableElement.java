package org.nextprot.pipeline.statement.muxdemux;


import org.nextprot.pipeline.statement.PipelineElement;


public interface DuplicableElement extends PipelineElement {

	/**
	 * Duplicate this element with a new capacity value
	 * @param newCapacity the new capacity
	 * @return a copy of itself
	 */
	PipelineElement duplicate(int newCapacity);

	/**
	 * @return a copy of itself
	 */
	default PipelineElement duplicate() {

		return duplicate(getCapacity());
	}
}
