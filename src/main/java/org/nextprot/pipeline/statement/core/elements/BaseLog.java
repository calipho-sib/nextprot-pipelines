package org.nextprot.pipeline.statement.core.elements;


import org.nextprot.commons.statements.Statement;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

import static org.nextprot.pipeline.statement.core.elements.Source.POISONED_STATEMENT;


public abstract class BaseLog {

	private static final String LOGS_FOLDER = "logs";

	private final PrintStream printStream;
	private final String threadName;

	public BaseLog(String threadName) throws FileNotFoundException {

		this(threadName, LOGS_FOLDER);
	}

	public BaseLog(String threadName, String path) throws FileNotFoundException {

		this(threadName, new PrintStream(new File(path + File.separator + threadName + ".log")));
	}

	public BaseLog(String threadName, PrintStream printStream) {

		this.threadName = threadName;
		this.printStream = printStream;
	}

	protected synchronized void sendMessage(String message) {

		printStream.println(threadName + ": " + message);
	}

	protected synchronized String getStatementId(Statement statement) {

		return ((statement == POISONED_STATEMENT) ? "POISONED_STATEMENT" : statement.getStatementId());
	}
}
