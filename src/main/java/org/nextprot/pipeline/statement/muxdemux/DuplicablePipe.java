package org.nextprot.pipeline.statement.muxdemux;


import org.nextprot.pipeline.statement.pipes.SinkPipe;
import org.nextprot.pipeline.statement.pipes.SourcePipe;

public interface DuplicablePipe {

	DuplicablePipe duplicate();
	SinkPipe getPipedInputPort();
	SourcePipe getPipedOutputPort();
}
