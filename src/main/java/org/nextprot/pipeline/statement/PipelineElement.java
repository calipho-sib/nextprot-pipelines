package org.nextprot.pipeline.statement;

import org.nextprot.pipeline.statement.pipes.SinkPipe;
import org.nextprot.pipeline.statement.pipes.SourcePipe;

import java.io.IOException;
import java.util.List;

public interface PipelineElement {

	String getName();
	void start(List<Thread> collector);
	void stop() throws IOException;
	void connect(PipelineElement element) throws IOException;
	SinkPipe getSinkPipe();
	SourcePipe getSourcePipe();
	int getCapacity();
}
