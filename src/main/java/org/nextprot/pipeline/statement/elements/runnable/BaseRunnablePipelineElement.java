package org.nextprot.pipeline.statement.elements.runnable;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.PipelineElement;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class BaseRunnablePipelineElement<E extends PipelineElement> implements RunnablePipelineElement {

	protected static final Statement END_OF_FLOW_TOKEN = null;
	private static int FLOW_INIT_NUMBER;

	private static synchronized int NEXT_FLOW_NUM() {
		return FLOW_INIT_NUMBER++;
	}

	protected final E pipelineElement;
	private final String name;
	private final int capacity;
	protected final ThreadLocal<FlowEventHandler> flowEventHandlerHolder = new ThreadLocal<>();

	public BaseRunnablePipelineElement(int capacity, E pipelineElement) {

		this.pipelineElement = pipelineElement;
		this.name = this.pipelineElement.getName()+ "-" + NEXT_FLOW_NUM();
		this.capacity = capacity;
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

			List<Statement> buffer = new ArrayList<>();
			eh.elementOpened(capacity);

			boolean endOfFlow = false;

			while (!endOfFlow) {

				endOfFlow = handleFlow(buffer);
				buffer.clear();
			}
			eh.endOfFlow();
		} catch (IOException e) {
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

	@Override
	public int capacity() {

		return capacity;
	}
}
