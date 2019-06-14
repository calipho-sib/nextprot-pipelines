package org.nextprot.pipeline.statement.elements.runnable;

import org.nextprot.commons.statements.Statement;

public interface FlowEventHandler {

	void elementOpened();
	void statementHandled(Statement statement);
	void endOfFlow();

	class Mute implements FlowEventHandler {

		@Override
		public void elementOpened() { }

		@Override
		public void statementHandled(Statement statement) { }

		@Override
		public void endOfFlow() { }
	}
}
