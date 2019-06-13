package org.nextprot.pipeline.statement.elements.runnable;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.PipelineElement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractRunnablePipelineElement<E extends PipelineElement> implements RunnablePipelineElement {

	protected static final Statement END_OF_FLOW_TOKEN = null;
	private static int FLOW_INIT_NUMBER;

	private static synchronized int NEXT_FLOW_NUM() {
		return FLOW_INIT_NUMBER++;
	}

	protected final E pipelineElement;
	protected final ThreadLocal<Boolean> endOfFlowHolder = ThreadLocal.withInitial(() -> false);
	private final String name;
	private final int capacity;

	public AbstractRunnablePipelineElement(int capacity, E pipelineElement) {

		this.pipelineElement = pipelineElement;
		this.name = this.pipelineElement.getName()+ "-" + NEXT_FLOW_NUM();
		this.capacity = capacity;
	}

	@Override
	public void run() {

		try {
			List<Statement> buffer = new ArrayList<>();
			elementOpened(capacity);

			while (!endOfFlowHolder.get()) {

				endOfFlowHolder.set(handleFlow(buffer));
				buffer.clear();
			}
			endOfFlow();
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

	@Override
	public void elementOpened(int capacity) { }

	@Override
	public void statementsHandled(int statements) { }

	@Override
	public void endOfFlow() { }
}
