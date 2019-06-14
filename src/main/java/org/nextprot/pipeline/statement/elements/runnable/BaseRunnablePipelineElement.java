package org.nextprot.pipeline.statement.elements.runnable;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.PipelineElement;

import java.io.FileNotFoundException;
import java.io.IOException;

public abstract class BaseRunnablePipelineElement<E extends PipelineElement> implements RunnablePipelineElement {

	public static final Statement END_OF_FLOW_TOKEN = new Statement();
	private static int FLOW_INIT_NUMBER;

	private static synchronized int NEXT_FLOW_NUM() {
		return FLOW_INIT_NUMBER++;
	}

	protected final E pipelineElement;
	private final String name;
	protected final ThreadLocal<FlowEventHandler> flowEventHandlerHolder = new ThreadLocal<>();

	public BaseRunnablePipelineElement(E pipelineElement) {

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

			eh.elementOpened();

			boolean endOfFlow = false;

			while (!endOfFlow) {

				endOfFlow = handleFlow();
			}
			eh.endOfFlow();
		} catch (Exception e) {
			System.err.println(Thread.currentThread().getName() +": "+e.getMessage());
		}
		finally {
			try {
				pipelineElement.unpipe();
			} catch (IOException e) {
				System.err.println(Thread.currentThread().getName() + ": could not stop, e=" + e.getMessage());
			}
		}
	}

	@Override
	public String getThreadName() {

		return name;
	}

	public int capacity() {

		return pipelineElement.getSinkPipePort().remainingCapacity();
	}
}
