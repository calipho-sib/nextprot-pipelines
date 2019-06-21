package org.nextprot.pipeline.statement.core.elements;


import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.core.elements.demux.DuplicableElement;

import java.util.concurrent.BlockingQueue;


public abstract class Sink extends BasePipelineElement<DuplicableElement> implements DuplicableElement {

	protected Sink() {

		super(null);
	}

	@Override
	public BlockingQueue<Statement> getSourceChannel() {

		throw new Error("It is a Sink element, can't connect to a PipelineElement through this channel!");
	}
}
