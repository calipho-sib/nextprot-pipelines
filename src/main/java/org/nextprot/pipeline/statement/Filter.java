package org.nextprot.pipeline.statement;


import org.nextprot.pipeline.statement.pipes.SinkPipe;
import org.nextprot.pipeline.statement.pipes.SourcePipe;

import java.io.IOException;

public interface Filter extends PipelineElement {

	/**
	 * Filter statements coming from input port to output port
	 *
	 * @param in  input port
	 * @param out output port
	 * @return false if end of flow token has been received
	 * @throws IOException
	 */
	boolean filter(SinkPipe in, SourcePipe out) throws IOException;
}
