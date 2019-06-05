package org.nextprot.pipeline.statement;


import org.nextprot.pipeline.statement.muxdemux.DuplicableElement;
import org.nextprot.pipeline.statement.ports.SinkPipePort;
import org.nextprot.pipeline.statement.ports.SourcePipePort;

import java.io.IOException;

public interface Filter extends PipelineElement, DuplicableElement {

	/**
	 * Filter statements coming from input port to output port
	 *
	 * @param in  input port
	 * @param out output port
	 * @return false if end of flow token has been received
	 * @throws IOException
	 */
	boolean filter(SinkPipePort in, SourcePipePort out) throws IOException;
}
