package org.nextprot.pipeline.statement;

import org.nextprot.pipeline.statement.ports.SinkPipePort;
import org.nextprot.pipeline.statement.ports.SourcePipePort;

import java.io.IOException;
import java.util.List;

public interface PipelineElement<E extends PipelineElement> {

	String getName();
	void start(List<Thread> collector);
	boolean hasStarted();
	void stop() throws IOException;
	void pipe(E element) throws IOException;

	/** @return the sink pipe port or null */
	SinkPipePort getSinkPipePort();

	/** @return the source pipe port or null */
	SourcePipePort getSourcePipePort();

	int getCapacity();

	E nextElement();
}
