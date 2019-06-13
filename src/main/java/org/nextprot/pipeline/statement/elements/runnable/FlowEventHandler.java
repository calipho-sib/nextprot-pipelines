package org.nextprot.pipeline.statement.elements.runnable;

public interface FlowEventHandler {

	void elementOpened(int capacity);
	void statementsHandled(int statementNum);
	void endOfFlow();

	class Mute implements FlowEventHandler {

		@Override
		public void elementOpened(int capacity) { }

		@Override
		public void statementsHandled(int statements) { }

		@Override
		public void endOfFlow() { }
	}
}
