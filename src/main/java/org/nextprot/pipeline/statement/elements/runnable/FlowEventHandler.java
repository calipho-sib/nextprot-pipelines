package org.nextprot.pipeline.statement.elements.runnable;

public interface FlowEventHandler {

	void elementOpened(int capacity);
	void statementsHandled(int statements);
	void endOfFlow();
}
