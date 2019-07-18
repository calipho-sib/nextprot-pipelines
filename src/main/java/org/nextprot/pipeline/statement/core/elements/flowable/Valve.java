package org.nextprot.pipeline.statement.core.elements.flowable;


import org.nextprot.pipeline.statement.core.PipelineElement;

/**
 * The statement valve actually do the job of handling the flow of statements
 * in a separate thread
 * @param <E>
 */
public interface Valve<E extends PipelineElement> extends Runnable {

	E getStage();

	/** handle the current flow and @return true if the flow has been poisoned */
	boolean handleFlow() throws Exception;

	FlowEventHandler getFlowEventHandler();
}
