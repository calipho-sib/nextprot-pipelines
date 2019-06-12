package org.nextprot.pipeline.statement;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.ports.SinkPipePort;
import org.nextprot.pipeline.statement.ports.SourcePipePort;

import java.io.IOException;
import java.util.List;

/**
 * This class represent an element of the pipeline
 * @param <E> the type of the next element to pipe into
 */
public interface PipelineElement<E extends PipelineElement> extends Runnable {

	/** @return the name of the thread */
	String getThreadName();

	/** Pipe the next element after this element */
	void pipe(E nextElement) throws IOException;

	/** @return the sink pipe port or null */
	SinkPipePort getSinkPipePort();

	/** @return the source pipe port or null */
	SourcePipePort getSourcePipePort();

	/** @return the next element connected to this element */
	E nextElement();

	/**
	 * Start the processing in a new thread and the following connected element
	 * @param collector collect running pipeline elements needed for thread management
	 */
	void start(List<Thread> collector);

	/** Stop the processing */
	void stop() throws IOException;

	void elementOpened(int capacity);

	/** handle the current flow and @return true if the flow ends */
	boolean handleFlow(List<Statement> buffer) throws IOException;

	void endOfFlow();
}
