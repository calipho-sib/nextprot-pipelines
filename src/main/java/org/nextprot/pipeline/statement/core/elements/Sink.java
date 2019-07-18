package org.nextprot.pipeline.statement.core.elements;


import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.core.elements.demux.DuplicableStage;

import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;


/**
 * A sink is the last stage of a pipeline
 *
 *        -----    ------
 * ... ==<  F  >==< Sink |
 *        -----    ------
 */
public abstract class Sink extends BaseStage<DuplicableStage> implements DuplicableStage {

	protected Sink() {

		super(null);
	}

	@Override
	public void pipe(DuplicableStage nextElement) {

		throw new Error("It is a SINK element, cannot pipe anything through this channel!");
	}

	@Override
	public BlockingQueue<Statement> getSourceChannel() {

		throw new Error("It is a SINK element, cannot pipe anything through this channel!");
	}

	@Override
	public Stream<DuplicableStage> nextStages() {

		return Stream.empty();
	}
}
