package org.nextprot.pipeline.statement.core.stage.flowable;


import org.nextprot.pipeline.statement.core.Stage;

/**
 * The statement valve actually do the job of handling the flow of statements
 * in a separate thread
 * @param <E>
 */
public interface RunnableStage<E extends Stage> extends Runnable {

	E getStage();

	/** handle the current flow and @return true if the flow has been poisoned */
	boolean handleFlow() throws Exception;

	FlowEventHandler getFlowEventHandler();
}
