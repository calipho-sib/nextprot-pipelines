package org.nextprot.pipeline.statement.elements.runnable;


import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.elements.BaseLog;

import java.io.FileNotFoundException;
import java.util.concurrent.BlockingQueue;

import static org.nextprot.pipeline.statement.elements.runnable.BaseFlowablePipelineElement.POISONED_STATEMENT;


public abstract class BaseFlowLog extends BaseLog implements FlowEventHandler {

	private int statementCount;

	public BaseFlowLog(String threadName) throws FileNotFoundException {

		super(threadName);
		this.statementCount = 0;
	}

	@Override
	public void statementHandled(Statement statement) {

		if (statement != POISONED_STATEMENT) {
			incrStatementCount();
		}
	}

	protected void statementHandled(String beginMessage, Statement statement, BlockingQueue<Statement> sinkChannel, BlockingQueue<Statement> sourceChannel) {

		String prefix;

		if (statement != POISONED_STATEMENT) {
			incrStatementCount();
			prefix = beginMessage + " statement " + getStatementId(statement);
		}
		else {
			prefix = "transmitting a poisoned statement";
		}

		sendMessage(prefix
				+ ((sinkChannel != null) ? " from sink channel #" + sinkChannel.hashCode() : "")
				+ ((sourceChannel != null) ? " to source channel #" + sourceChannel.hashCode() : ""));
	}

	private int incrStatementCount() {
		return statementCount++;
	}

	protected int getStatementCount() {
		return statementCount;
	}
}
