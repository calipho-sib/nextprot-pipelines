package org.nextprot.pipeline.statement.core.elements.source;

import java.io.IOException;
import java.util.List;

/**
 * Pump elements of a pipeline
 * @param <E> element type
 */
public interface Pump<E> {

	/** @return the next statement or null if not more to pump */
	E pump() throws IOException;

	int capacity();

	int pump(List<E> collector) throws IOException;
	boolean isEmpty() throws IOException;
	void stop() throws IOException;;
}
