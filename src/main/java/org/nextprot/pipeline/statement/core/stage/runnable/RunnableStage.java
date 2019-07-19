package org.nextprot.pipeline.statement.core.stage.runnable;


import org.nextprot.pipeline.statement.core.Stage;

/**
 * The statement stage actually do the job of handling the flow of statements
 * in a separate thread
 * @param <E>
 */
public interface RunnableStage<E extends Stage> extends Runnable {

	E getStage();

	/** handle the current flow and @return true if the flow has been poisoned */
	boolean handleFlow() throws Exception;

	FlowEventHandler getFlowEventHandler();
}
