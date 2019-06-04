package org.nextprot.pipeline.statement.pipes;


import org.nextprot.pipeline.statement.Pipe;
import org.nextprot.pipeline.statement.ports.PipedInputPort;

public abstract class Sink extends BasePipe {

	protected Sink(int sectionWidth) {

		super(sectionWidth, new PipedInputPort(sectionWidth));
	}

	@Override
	public void connect(Pipe receiver) {

		throw new Error("It is a sink, can't connect to a PipedOutputPort!");
	}
}
