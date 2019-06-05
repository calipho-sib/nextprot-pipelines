package org.nextprot.pipeline.statement.elements;


import org.nextprot.pipeline.statement.PipelineElement;
import org.nextprot.pipeline.statement.muxdemux.DuplicableElement;
import org.nextprot.pipeline.statement.pipes.SinkPipe;
import org.nextprot.pipeline.statement.pipes.SourcePipe;


public abstract class Sink extends BasePipelineElement implements DuplicableElement {

	protected Sink(int capacity) {

		super(capacity, new SinkPipe(capacity));
	}

	@Override
	public void connect(PipelineElement receiver) {

		throw new Error("It is a sink, can't connect to a PipelineElement through this pipe!");
	}

	@Override
	public SourcePipe getSourcePipe() {

		throw new Error("It is a sink, can't connect to a PipelineElement through this pipe!");
	}
}
