package org.nextprot.pipeline.statement.elements;


import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.elements.runnable.FlowEventHandler;

import java.util.concurrent.BlockingQueue;

/**
 * This filter just transmit statements from PipedInputPort to PipedOutputPort
 * and take a nap
 */
public class NarcolepticFilter extends BaseFilter {

	private final long takeANapInMillis;

	public NarcolepticFilter(int capacity) {

		this(capacity, -1);
	}

	public NarcolepticFilter(int capacity, long takeANapInMillis) {

		super(capacity);
		this.takeANapInMillis = takeANapInMillis;
	}

	@Override
	public NarcolepticFilter duplicate(int capacity) {

		return new NarcolepticFilter(capacity, this.takeANapInMillis);
	}

	@Override
	public Flowable newFlowable() {

		return new Flowable(this);
	}

	private static class Flowable extends FlowableFilter<NarcolepticFilter> {

		private final long napTime;

		private Flowable(NarcolepticFilter pipelineElement) {
			super(pipelineElement);

			napTime = pipelineElement.takeANapInMillis;
		}

		@Override
		public boolean filter(BlockingQueue<Statement> in, BlockingQueue<Statement> out) throws Exception {

			FlowEventHandler eh = flowEventHandlerHolder.get();

			Statement current = in.take();
			eh.statementHandled(current);

			takeANap(napTime);

			out.put(current);

			return current == END_OF_FLOW_STATEMENT;
		}

		private void takeANap(long nap) {

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
