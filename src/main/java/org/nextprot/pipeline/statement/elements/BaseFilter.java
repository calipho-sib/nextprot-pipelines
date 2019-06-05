package org.nextprot.pipeline.statement.elements;



import org.nextprot.pipeline.statement.Filter;
import org.nextprot.pipeline.statement.muxdemux.DuplicableElement;
import org.nextprot.pipeline.statement.pipes.SinkPipe;

import java.io.IOException;


public abstract class BaseFilter extends BasePipelineElement implements Filter, DuplicableElement {

	private final ThreadLocal<Boolean> endOfFlow;

	protected BaseFilter(int capacity) {

		super(capacity, new SinkPipe(capacity));
		endOfFlow = ThreadLocal.withInitial(() -> false);
	}

	@Override
	public void handleFlow() throws IOException {

		while (!endOfFlow.get()) {

			endOfFlow.set(filter(getSinkPipe(), getSourcePipe()));
		}
	}
}
