package org.nextprot.pipeline.statement.core;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.core.stage.flowable.RunnableStage;

import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;

/**
 * This class represent an element of the pipeline.
 *
 * A stage can:
 * 1. consume Statements from the previous stage via a sink channel
 * 2. produce Statements to the next stage(s) via source channel(s)
 *
 * A Stage should run Channels do the synchronization between Threads
 *
 * @param <E> the next stage type to pipe to
 */
public interface Stage<E extends Stage> {

	/** @return the name of stage */
	String getName();

	/** Pipe to the next stage */
	void pipe(E stage);

	/** @return the sink pipe port or null if absent */
	BlockingQueue<Statement> getSinkChannel();

	void setSinkChannel(BlockingQueue<Statement> sinkChannel);

	/** @return the source pipe port or null if absent */
	BlockingQueue<Statement> getSourceChannel();

	/** @return the next piped stage(s) */
	Stream<E> nextStages();

	/** @return the first piped stage */
	E nextStage();

	default int countStages() {

		return (int) nextStages().count();
	}

	/**
	 * Disconnect sink and source pipes
	 */
	void close();

	/**
	 * @return a new runnable stage that will handle the flow of statement in a separate Thread
	 */
	RunnableStage newRunnableStage();
}
