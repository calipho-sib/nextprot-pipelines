package org.nextprot.pipeline.statement.core.elements.filter;


import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.core.elements.flowable.FlowEventHandler;
import org.nextprot.pipeline.statement.core.elements.demux.DuplicableStage;

import java.io.FileNotFoundException;
import java.util.concurrent.BlockingQueue;

import static org.nextprot.pipeline.statement.core.elements.Source.POISONED_STATEMENT;

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
	public DuplicableStage duplicate(int newCapacity) {

		return new NarcolepticFilter(newCapacity, this.takeANapInMillis);
	}

	@Override
	public RunnableStage newRunnableStage() {

		return new RunnableStage(this);
	}

	private static class RunnableStage extends FilterRunnableStage<NarcolepticFilter> {

		private final long napTime;

		private RunnableStage(NarcolepticFilter pipelineElement) {
			super(pipelineElement);

			napTime = pipelineElement.takeANapInMillis;
		}

		@Override
		public boolean filter(BlockingQueue<Statement> in, BlockingQueue<Statement> out) throws Exception {

			FilterFlowLog eh = (FilterFlowLog) getFlowEventHandler();

			Statement current = in.take();
			eh.statementHandled(current, in, out);

			takeANap(napTime);

			out.put(current);

			return current == POISONED_STATEMENT;
		}

		@Override
		protected FlowEventHandler createFlowEventHandler() throws FileNotFoundException {

			return new FilterFlowLog(Thread.currentThread().getName());
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
