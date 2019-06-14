package org.nextprot.pipeline.statement.elements;


import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.elements.runnable.FlowEventHandler;
import org.nextprot.pipeline.statement.ports.SinkPipePort;
import org.nextprot.pipeline.statement.ports.SourcePipePort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

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
		public boolean filter(BlockingQueue<Statement> in, BlockingQueue<Statement> out) throws Exception {

			FlowEventHandler eh = flowEventHandlerHolder.get();

			Statement current = in.take();
			eh.statementHandled(current);

			takeANap(pipelineElement.getTakeANapInMillis());

			out.put(current);

			return current == END_OF_FLOW_TOKEN;
		}

		private void takeANap(int nap) {

			if (nap > 0) {
				try {
					Thread.sleep(nap);
				} catch (InterruptedException e) {
					System.err.println(e.getMessage());
				}
			}
		}
	}
}
