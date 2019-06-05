package org.nextprot.pipeline.statement;

import org.nextprot.pipeline.statement.ports.SinkPipePort;
import org.nextprot.pipeline.statement.ports.SourcePipePort;

import java.io.IOException;
import java.util.List;

public interface PipelineElement {

	String getName();
	void start(List<Thread> collector);
	boolean hasStarted();
	void stop() throws IOException;
	void pipe(PipelineElement element) throws IOException;

	/** @return the sink pipe port or null */
	SinkPipePort getSinkPipePort();

	/** @return the source pipe port or null */
	SourcePipePort getSourcePipePort();

	int getCapacity();

	PipelineElement nextElement();
}
