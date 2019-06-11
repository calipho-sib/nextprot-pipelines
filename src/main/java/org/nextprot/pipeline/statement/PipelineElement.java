package org.nextprot.pipeline.statement;

import org.nextprot.pipeline.statement.ports.SinkPipePort;
import org.nextprot.pipeline.statement.ports.SourcePipePort;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

/**
 * This class represent an element of the pipeline
 * @param <E> the type of the next element to pipe into
 */
public interface PipelineElement<E extends PipelineElement> extends Runnable {

	/** @return the name of the thread */
	String getThreadName();

	/** Pipe the next element after this element */
	void pipe(E nextElement) throws IOException;

	/** @return the sink pipe port or null */
	SinkPipePort getSinkPipePort();

	/** @return the source pipe port or null */
	SourcePipePort getSourcePipePort();

	/** @return the next element connected to this element */
	E nextElement();

	/**
	 * Start the element and the following connected element
	 * @param collector collect running pipeline elements needed for thread management
	 */
	void start(List<Thread> collector);

	/** Stop the processing */
	void stop() throws IOException;

	/** @return the log stream */
	PrintStream getLogStream();

	/** log a message */
	default void printlnTextInLog(String message) {

		getLogStream().println(getThreadName()+":" + message);
	}

	/** @return output stream for log messages */
	default PrintStream createLogStream() {
		try {
			return new PrintStream(new File("logs"+File.separator+getThreadName()+".log"));
		} catch (FileNotFoundException e) {
			throw new IllegalStateException("Cannot create log file for pipeline element "+ getThreadName(), e);
		}
	}
}
