package org.nextprot.pipeline.statement.elements.runnable;


import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.elements.BaseLog;

import java.io.FileNotFoundException;

import static org.nextprot.pipeline.statement.elements.runnable.BaseFlowablePipelineElement.END_OF_FLOW_STATEMENT;


public abstract class BaseFlowLog extends BaseLog implements FlowEventHandler {

	private int statementCount;

	public BaseFlowLog(String threadName) throws FileNotFoundException {

		super(threadName);
		this.statementCount = 0;
	}

	@Override
	public void statementHandled(Statement statement) {

		if (statement != END_OF_FLOW_STATEMENT) {
			incrStatementCount();
		}
	}

	protected synchronized int incrStatementCount() {
		return statementCount++;
	}

	protected synchronized int getStatementCount() {
		return statementCount;
	}
}
