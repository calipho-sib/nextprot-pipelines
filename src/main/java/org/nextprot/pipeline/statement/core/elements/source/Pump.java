package org.nextprot.pipeline.statement.core.elements.source;


import java.io.IOException;

/**
 * Pump elements from a source
 * @param <E> element type
 */
public interface Pump<E> {

	/** @return the next statement or null if not more to pump */
	E pump() throws PumpException;

	/** @return true if pump cannot provide more elements */
	boolean isSourceEmpty() throws PumpException;

	/** Stop the pump and release the resources */
	void stop() throws PumpException;

	class PumpException extends RuntimeException {

		public PumpException(String message) {

			super(message);
		}

		public PumpException(String message, IOException e) {

			super(message + ", error: " + e.getMessage());
		}
	}
}
