package org.nextprot.pipeline.statement.elements;


import org.nextprot.pipeline.statement.PipelineElement;
import org.nextprot.pipeline.statement.muxdemux.DuplicableElement;
import org.nextprot.pipeline.statement.ports.SourcePipePort;


public abstract class Sink extends BasePipelineElement implements DuplicableElement {

	protected Sink(int capacity) {

		super(capacity);
	}

	@Override
	public void pipe(PipelineElement receiver) {

		throw new Error("It is a sink, can't connect to a PipelineElement through this pipe!");
	}

	@Override
	public SourcePipePort getSourcePipePort() {

		throw new Error("It is a sink, can't connect to a PipelineElement through this pipe!");
	}
}
