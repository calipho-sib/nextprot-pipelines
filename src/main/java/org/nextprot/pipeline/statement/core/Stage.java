package org.nextprot.pipeline.statement.core;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.core.stage.runnable.RunnableStage;

import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;

/**
 * A Stage represents an active element of the pipeline.
 *
 * A stage can:
 *
 * 1. consume Statements coming from the previous stage via a Sink Channel
 * 2. produce Statements to the next stage(s) via Source Channel(s)
 *
 * A stage should execute into a separate Thread through RunnableStage
 *
 * @param <E> the next stage type to pipe to
 */
public interface Stage<E extends Stage> {

	/** @return the name of stage */
	String getName();

	/** Pipe to the next stage */
	void pipe(E stage);

	/** @return the sink channel where Statements are consumed or null if absent */
	BlockingQueue<Statement> getSinkChannel();

	void setSinkChannel(BlockingQueue<Statement> sinkChannel);

	/** @return the source channel where Statements are produced or null if absent */
	BlockingQueue<Statement> getSourceChannel();

	/** @return the next piped stage(s) */
	Stream<E> getPipedStages();

	/** @return the first piped stage */
	E getFirstPipedStage();

	default int countPipedStages() {

		return (int) getPipedStages().count();
	}

	/** Disconnect sink and source channels */
	void close();

	/** @return a new runnable stage that will handle the flow of statement in a separate Thread */
	RunnableStage newRunnableStage();
}
