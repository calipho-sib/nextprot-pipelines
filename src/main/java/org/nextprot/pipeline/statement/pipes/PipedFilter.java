package org.nextprot.pipeline.statement.pipes;



import org.nextprot.pipeline.statement.Filter;
import org.nextprot.pipeline.statement.ports.PipedInputPort;

import java.io.IOException;


public abstract class PipedFilter extends BasePipe implements Filter {

	private final ThreadLocal<Boolean> endOfFlow;

	protected PipedFilter(int sectionWidth) {

		super(sectionWidth, new PipedInputPort(sectionWidth));
		endOfFlow = ThreadLocal.withInitial(() -> false);
	}

	@Override
	public void handleFlow() throws IOException {

		while (!endOfFlow.get()) {

			endOfFlow.set(filter(in, out));
		}
	}
}
