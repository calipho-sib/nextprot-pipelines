package org.nextprot.pipeline.statement.core.elements.flowable;


import org.nextprot.pipeline.statement.core.PipelineElement;

public interface FlowablePipelineElement<E extends PipelineElement> extends Runnable {

	/** handle the current flow and @return true if the flow has been poisoned */
	boolean handleFlow(E pipelineElement) throws Exception;

	/** @return the name of thread handling this flow */
	String getThreadName();

	FlowEventHandler getFlowEventHandler();
}
