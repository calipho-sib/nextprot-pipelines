package org.nextprot.pipeline.statement.core.stage.filter;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.core.stage.runnable.BaseFlowLog;

import java.io.FileNotFoundException;
import java.util.concurrent.BlockingQueue;

public class FilterFlowLog extends BaseFlowLog {

	public FilterFlowLog(String threadName) throws FileNotFoundException {

		super(threadName);
	}

	@Override
	public void beginOfFlow() {

		sendMessage("start filtering flow");
	}

	public void statementHandled(Statement statement, BlockingQueue<Statement> sinkChannel,
	                             BlockingQueue<Statement> sourceChannel) {

		statementHandled("filtering", statement, sinkChannel, sourceChannel);
	}

	@Override
	public void endOfFlow() {

		sendMessage(getStatementCount()+ " healthy statements filtered");
	}
}
