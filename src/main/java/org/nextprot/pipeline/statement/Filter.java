package org.nextprot.pipeline.statement;


import org.nextprot.pipeline.statement.ports.PipedInputPort;
import org.nextprot.pipeline.statement.ports.PipedOutputPort;

import java.io.IOException;

public interface Filter extends Pipe {

	/**
	 * Filter statements coming from input port to output port
	 *
	 * @param in  input port
	 * @param out output port
	 * @return false if end of flow token has been received
	 * @throws IOException
	 */
	boolean filter(PipedInputPort in, PipedOutputPort out) throws IOException;
}
