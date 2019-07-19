package org.nextprot.pipeline.statement.core.stage.flowable;

import org.nextprot.commons.statements.Statement;

public interface FlowEventHandler {

	void beginOfFlow();
	void statementHandled(Statement statement);
	void endOfFlow();

	class Mute implements FlowEventHandler {

		@Override
		public void beginOfFlow() { }

		@Override
		public void statementHandled(Statement statement) { }

		@Override
		public void endOfFlow() { }
	}
}
