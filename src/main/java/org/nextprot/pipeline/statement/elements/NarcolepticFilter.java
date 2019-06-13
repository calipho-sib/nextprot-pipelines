package org.nextprot.pipeline.statement.elements;


import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.elements.runnable.FlowEventHandler;
import org.nextprot.pipeline.statement.ports.SinkPipePort;
import org.nextprot.pipeline.statement.ports.SourcePipePort;

import java.io.IOException;

/**
 * This filter just transmit statements from PipedInputPort to PipedOutputPort
 * and take a nap
 */
public class NarcolepticFilter extends BaseFilter {

	private final int takeANapInMillis;

	public NarcolepticFilter(int capacity) {

		this(capacity, -1);
	}

	public NarcolepticFilter(int capacity, int takeANapInMillis) {

		super(capacity);
		this.takeANapInMillis = takeANapInMillis;
	}

	@Override
	public NarcolepticFilter duplicate(int capacity) {

		return new NarcolepticFilter(capacity, this.takeANapInMillis);
	}

	public int getTakeANapInMillis() {
		return takeANapInMillis;
	}

	@Override
	public RunnableNarcolepticFilter newRunnableElement() {

		return new RunnableNarcolepticFilter(this);
	}

	private static class RunnableNarcolepticFilter extends RunnableFilter<NarcolepticFilter> {

		private RunnableNarcolepticFilter(NarcolepticFilter pipelineElement) {
			super(pipelineElement);
		}

		@Override
		public boolean filter(SinkPipePort in, SourcePipePort out) throws IOException {

			FlowEventHandler eh = flowEventHandlerHolder.get();

			Statement[] buffer = new Statement[in.capacity()];

			int numOfStatements = in.read(buffer, 0, in.capacity());

			for (int i=0 ; i<numOfStatements ; i++) {

				out.write(buffer[i]);

				if (buffer[i] == END_OF_FLOW_TOKEN) {

					return true;
				} else {
					int nap = pipelineElement.getTakeANapInMillis();
					if (nap > 0) {
						try {
							Thread.sleep(nap);
						} catch (InterruptedException e) {
							System.err.println(e.getMessage());
						}
					}
				}
			}

			eh.statementsHandled(numOfStatements);

			return false;
		}
	}
}
