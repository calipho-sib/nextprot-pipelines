package org.nextprot.pipeline.statement.core;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.core.elements.flowable.FlowablePipelineElement;

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

	/** Pipe the sink element after this element */
	void pipe(E sink);

	/** @return the sink pipe port or null */
	BlockingQueue<Statement> getSinkChannel();
	void setSinkChannel(BlockingQueue<Statement> sinkChannel);

	/** @return the source pipe port or null */
	BlockingQueue<Statement> getSourceChannel();

	/** @return the next piped element */
	E nextSink();

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

	FlowablePipelineElement newFlowable();
}
