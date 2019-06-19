package org.nextprot.pipeline.statement.elements.runnable;


import org.nextprot.pipeline.statement.PipelineElement;

import java.io.FileNotFoundException;

public interface FlowablePipelineElement<E extends PipelineElement> extends Runnable {

	/** handle the current flow and @return true if the flow ends */
	boolean handleFlow(E pipelineElement) throws Exception;

	/** @return the name of thread handling this flow */
	String getThreadName();

	int capacity();

	FlowEventHandler createEventHandler() throws FileNotFoundException;
}
