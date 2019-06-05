package org.nextprot.pipeline.statement.elements;



import org.nextprot.pipeline.statement.Filter;
import org.nextprot.pipeline.statement.muxdemux.DuplicableElement;

import java.io.IOException;


public abstract class BaseFilter extends BasePipelineElement<DuplicableElement> implements Filter {

	private final ThreadLocal<Boolean> endOfFlow;

	protected BaseFilter(int capacity) {

		super(capacity);
		endOfFlow = ThreadLocal.withInitial(() -> false);
	}

	@Override
	public void handleFlow() throws IOException {

		while (!endOfFlow.get()) {

			endOfFlow.set(filter(getSinkPipePort(), getSourcePipePort()));
		}
	}
}
