package org.nextprot.pipeline.statement.elements;

public interface EventHandler {

	void sinkPipePortUnpiped();
	void sourcePipePortUnpiped();
	void elementClosed();
}
