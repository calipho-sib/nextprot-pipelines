package org.nextprot.pipeline.statement.muxdemux;


import org.nextprot.pipeline.statement.ports.PipedInputPort;
import org.nextprot.pipeline.statement.ports.PipedOutputPort;

public interface DuplicablePipe {

	DuplicablePipe duplicate();
	PipedInputPort getPipedInputPort();
	PipedOutputPort getPipedOutputPort();
}
