package org.nextprot.pipeline.statement.pipes;


import org.nextprot.pipeline.statement.Pipe;
import org.nextprot.pipeline.statement.Sink;

public abstract class PipedSink extends ConcurrentPipe implements Sink {

	protected PipedSink(int sectionWidth) {

		super(sectionWidth);
	}

	@Override
	public void connect(Pipe receiver) {

		throw new Error("It is a sink, can't connect to a PipedOutputPort!");
	}
}
