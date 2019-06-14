package org.nextprot.pipeline.statement.elements;


import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.muxdemux.DuplicableElement;

import java.util.concurrent.BlockingQueue;


public abstract class Sink extends BasePipelineElement<DuplicableElement> implements DuplicableElement {

	protected Sink() {

		super(null);
	}

	@Override
	public BlockingQueue<Statement> getSourcePipePort() {

		throw new Error("It is a sink, can't connect to a PipelineElement through this pipe!");
	}
}
