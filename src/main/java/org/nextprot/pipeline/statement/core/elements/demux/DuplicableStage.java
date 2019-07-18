package org.nextprot.pipeline.statement.core.elements.demux;


import org.nextprot.pipeline.statement.core.Stage;


public interface DuplicableStage extends Stage<DuplicableStage> {

	/**
	 * Duplicate this stage with a new capacity
	 * @param newCapacity the new capacity
	 * @return a copy of itself
	 */
	DuplicableStage duplicate(int newCapacity);
}
