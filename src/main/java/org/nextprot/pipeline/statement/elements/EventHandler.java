package org.nextprot.pipeline.statement.elements;

public interface EventHandler {

	void elementOpened(int capacity);
	void statementsHandled(int statements);
	void endOfFlow();
	void sinkPipePortClosed();
	void sourcePipePortClosed();
	void elementClosed();
}
