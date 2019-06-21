package org.nextprot.pipeline.statement.core.elements.flowable;

import org.nextprot.pipeline.statement.core.PipelineElement;


public abstract class BaseFlowablePipelineElement<E extends PipelineElement> implements FlowablePipelineElement<E> {

	private static int FLOWABLE_NUMBER = 0;

	private static synchronized int NEXT_FLOWABLE_NUM() {
		return FLOWABLE_NUMBER++;
	}

	// Warning: this is a mutable object that should perform synchronizations for this Flowable to remain thread-safe
	private final E pipelineElement;
	private final String name;
	private FlowEventHandler flowEventHandler;

	public BaseFlowablePipelineElement(E pipelineElement) {

		this.pipelineElement = pipelineElement;
		this.name = this.pipelineElement.getName()+ "-" + NEXT_FLOWABLE_NUM();
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
	public void run() {

		try {
			flowEventHandler = createFlowEventHandler();
			flowEventHandler.beginOfFlow();

			boolean endOfFlow = false;

			while (!endOfFlow) {

				endOfFlow = handleFlow(pipelineElement);
			}
			flowEventHandler.endOfFlow();
		} catch (Exception e) {
			System.err.println("EXCEPTION thrown by "+Thread.currentThread().getName() +": "+e.getMessage());
		}
	}

	@Override
	public String getThreadName() {

		return name;
	}
}
