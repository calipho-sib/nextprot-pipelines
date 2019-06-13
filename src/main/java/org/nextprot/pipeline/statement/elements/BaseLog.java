package org.nextprot.pipeline.statement.elements;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

public abstract class BaseLog {

	private final PrintStream printStream;
	private final String threadName;

	public BaseLog(String threadName) {

		this(threadName, System.out);
	}

	public BaseLog(String threadName, String path) throws FileNotFoundException {

		this(threadName, new PrintStream(new File(path+File.separator+threadName+".log")));
	}

	public BaseLog(String threadName, PrintStream printStream) {

		this.threadName = threadName;
		this.printStream = printStream;
	}

	protected synchronized void sendMessage(String message) {

		printStream.println(threadName+":" + message);
	}
}
