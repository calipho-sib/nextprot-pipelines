package org.nextprot.pipeline.statement.elements.runnable;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.PipelineElement;

import java.io.FileNotFoundException;

public abstract class BaseFlowablePipelineElement<E extends PipelineElement> implements FlowablePipelineElement<E> {

	public static final Statement END_OF_FLOW_STATEMENT = new Statement();
	private static int FLOW_INIT_NUMBER;

	private static synchronized int NEXT_FLOW_NUM() {
		return FLOW_INIT_NUMBER++;
	}

	private final E pipelineElement;
	private final String name;
	protected final ThreadLocal<FlowEventHandler> flowEventHandlerHolder = new ThreadLocal<>();

	public BaseFlowablePipelineElement(E pipelineElement) {

		this.pipelineElement = pipelineElement;
		this.name = this.pipelineElement.getName()+ "-" + NEXT_FLOW_NUM();
	}

	@Override
	public FlowEventHandler createEventHandler() throws FileNotFoundException {

		return new FlowEventHandler.Mute();
	}

	@Override
	public void run() {

		try {
			FlowEventHandler eh = createEventHandler();
			flowEventHandlerHolder.set(eh);

			eh.beginOfFlow();

			boolean endOfFlow = false;

			while (!endOfFlow) {

				endOfFlow = handleFlow(pipelineElement);
			}
			eh.endOfFlow();
		} catch (Exception e) {
			System.err.println(Thread.currentThread().getName() +": "+e.getMessage());
		}
	}

	@Override
	public String getThreadName() {

		return name;
	}

	public int capacity() {

		return pipelineElement.getSinkChannel().remainingCapacity();
	}
}
