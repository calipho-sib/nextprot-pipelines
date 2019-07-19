package org.nextprot.pipeline.statement.core.stage.flowable;

import org.nextprot.pipeline.statement.core.Stage;


public abstract class BaseRunnableStage<E extends Stage> implements RunnableStage<E> {

	// Warning: this is a mutable object that should perform synchronizations for this Flowable to remain thread-safe
	private final E pipelineElement;
	private FlowEventHandler flowEventHandler;

	public BaseRunnableStage(E pipelineElement) {

		this.pipelineElement = pipelineElement;
	}

	protected FlowEventHandler createFlowEventHandler() throws Exception {

		return new FlowEventHandler.Mute();
	}

	/* Thread-confined: owned exclusively by and confined to one Flowable thread */
	@Override
	public FlowEventHandler getFlowEventHandler() {

		return flowEventHandler;
	}

	@Override
	public E getStage() {

		return pipelineElement;
	}

	@Override
	public void run() {

		try {
			flowEventHandler = createFlowEventHandler();
			flowEventHandler.beginOfFlow();

			boolean endOfFlow = false;

			while (!endOfFlow) {

				endOfFlow = handleFlow();
			}
			flowEventHandler.endOfFlow();
		} catch (Exception e) {
			System.err.println("EXCEPTION thrown by "+Thread.currentThread().getName() +": "+e.getMessage());
		}
	}
}
