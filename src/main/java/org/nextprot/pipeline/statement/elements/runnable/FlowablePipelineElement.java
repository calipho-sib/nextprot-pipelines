package org.nextprot.pipeline.statement.elements.runnable;


import java.io.FileNotFoundException;

public interface FlowablePipelineElement extends Runnable {

	/** handle the current flow and @return true if the flow ends */
	boolean handleFlow() throws Exception;

	/** @return the name of thread handling this flow */
	String getThreadName();

	int capacity();

	FlowEventHandler createEventHandler() throws FileNotFoundException;
}
