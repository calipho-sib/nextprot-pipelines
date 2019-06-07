package org.nextprot.pipeline.statement;

import org.nextprot.pipeline.statement.ports.SinkPipePort;
import org.nextprot.pipeline.statement.ports.SourcePipePort;

import java.io.IOException;
import java.util.List;

/**
 * This class represent an element of the pipeline
 * @param <E> the type of the next element to pipe into
 */
public interface PipelineElement<E extends PipelineElement> {

	String getName();

	/** Pipe the next element into this element */
	void pipe(E nextElement) throws IOException;

	/** @return the sink pipe port or null */
	SinkPipePort getSinkPipePort();

	/** @return the source pipe port or null */
	SourcePipePort getSourcePipePort();

	/** @return the next element connected to this element */
	E nextElement();

	/** Start the processing */
	void start(List<Thread> collector);

	/** Stop the processing */
	void stop() throws IOException;

	/** @return true if has started*/
	boolean hasStarted();
}
