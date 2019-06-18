package org.nextprot.pipeline.statement;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.elements.ElementEventHandler;
import org.nextprot.pipeline.statement.elements.runnable.RunnablePipelineElement;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * This class represent an element of the pipeline
 * @param <E> the type of the next element to pipe into
 */
public interface PipelineElement<E extends PipelineElement> {

	/** @return the name of pipeline element */
	String getName();

	/** Pipe the next element after this element */
	void pipe(E nextElement);

	/** @return the sink pipe port or null */
	BlockingQueue<Statement> getSinkPipePort();
	void setSinkPipePort(BlockingQueue<Statement> queue);

	/** @return the source pipe port or null */
	BlockingQueue<Statement> getSourcePipePort();

	/** @return the next element connected to this element */
	E nextElement();

	/**
	 * Open the flow processing in a new thread and open also subsequent pipeline elements
	 * @param collector collect the running threads for management
	 */
	void openValves(List<Thread> collector);

	/**
	 * Disconnect sink and source pipes
	 * @throws IOException
	 */
	void closeValves() throws IOException;

	RunnablePipelineElement newRunnableElement();

	ElementEventHandler createEventHandler() throws FileNotFoundException;

	default boolean hasNextElement() {

		return nextElement() != null;
	}
}
