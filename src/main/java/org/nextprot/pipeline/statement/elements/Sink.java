package org.nextprot.pipeline.statement.elements;


import org.nextprot.pipeline.statement.muxdemux.DuplicableElement;
import org.nextprot.pipeline.statement.ports.SinkPipePort;
import org.nextprot.pipeline.statement.ports.SourcePipePort;


public abstract class Sink extends BasePipelineElement<DuplicableElement> implements DuplicableElement {

	protected Sink(int capacity) {

		super(capacity, new SinkPipePort(capacity), null);
	}

	@Override
	public SourcePipePort getSourcePipePort() {

		throw new Error("It is a sink, can't connect to a PipelineElement through this pipe!");
	}
}
