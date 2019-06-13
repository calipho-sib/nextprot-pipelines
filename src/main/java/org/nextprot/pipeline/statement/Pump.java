package org.nextprot.pipeline.statement;

import java.io.IOException;
import java.util.List;

/**
 * Pump elements of a pipeline
 * @param <E> element type
 */
public interface Pump<E> {

	E pump() throws IOException;
	int capacity();
	int pump(List<E> collector) throws IOException;
	boolean isEmpty() throws IOException;
	void stop() throws IOException;;
}
