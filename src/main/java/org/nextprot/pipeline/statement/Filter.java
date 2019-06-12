package org.nextprot.pipeline.statement;


import org.nextprot.pipeline.statement.muxdemux.DuplicableElement;
import org.nextprot.pipeline.statement.ports.SinkPipePort;
import org.nextprot.pipeline.statement.ports.SourcePipePort;

import java.io.IOException;

public interface Filter extends PipelineElement<DuplicableElement>, DuplicableElement {

	/**
	 * Filter statements coming from input port to output port
	 *
	 * @param in  input port
	 * @param out output port
	 * @return true if the flow has ended
	 * @throws IOException
	 */
	boolean filter(SinkPipePort in, SourcePipePort out) throws IOException;
}
