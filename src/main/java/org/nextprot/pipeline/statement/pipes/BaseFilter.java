package org.nextprot.pipeline.statement.pipes;



import org.nextprot.pipeline.statement.Filter;
import org.nextprot.pipeline.statement.ports.PipedInputPort;

import java.io.IOException;


public abstract class BaseFilter extends BasePipe implements Filter {

	private final ThreadLocal<Boolean> endOfFlow;

	protected BaseFilter(int sectionWidth) {

		super(sectionWidth, new PipedInputPort(sectionWidth));
		endOfFlow = ThreadLocal.withInitial(() -> false);
	}

	@Override
	public void handleFlow() throws IOException {

		while (!endOfFlow.get()) {

			endOfFlow.set(filter(inputPort, outputPort));
		}
	}
}
