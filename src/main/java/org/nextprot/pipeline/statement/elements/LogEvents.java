package org.nextprot.pipeline.statement.elements;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

public abstract class LogEvents implements EventHandler {

	private final PrintStream printStream;
	private final String threadName;

	public LogEvents(String threadName) {

		this(threadName, System.out);
	}

	public LogEvents(String threadName, String path) throws FileNotFoundException {

		this(threadName, new PrintStream(new File(path+File.separator+threadName+".log")));
	}

	public LogEvents(String threadName, PrintStream printStream) {

		this.threadName = threadName;
		this.printStream = printStream;
	}

	protected void sendMessage(String message) {

		printStream.println(threadName+":" + message);
	}

	//@Override
	public void pipeElementOpened(int capacity) {
		sendMessage("opened (capacity=" + capacity + ")");
	}

	//@Override
	public void endOfFlow() {
		sendMessage("end of flow");
	}

	@Override
	public void sinkPipePortUnpiped() {

		sendMessage("sink pipe port closed");
	}

	@Override
	public void sourcePipePortUnpiped() {

		sendMessage("source pipe port closed");
	}

	//@Override
	public void pipeElementClosed() {

		sendMessage("closed");
	}
}
