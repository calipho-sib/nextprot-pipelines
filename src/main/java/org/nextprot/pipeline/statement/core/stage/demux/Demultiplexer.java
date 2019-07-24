package org.nextprot.pipeline.statement.core.stage.demux;


import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.core.stage.BaseStage;
import org.nextprot.pipeline.statement.core.stage.DuplicableStage;
import org.nextprot.pipeline.statement.core.stage.ElementEventHandler;
import org.nextprot.pipeline.statement.core.stage.runnable.BaseFlowLog;
import org.nextprot.pipeline.statement.core.stage.runnable.BaseRunnableStage;
import org.nextprot.pipeline.statement.core.stage.runnable.FlowEventHandler;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;

import static org.nextprot.pipeline.statement.core.stage.Source.POISONED_STATEMENT;

/**
 * De-multiplexer receives statements via one source stage and distributes them to multiple sink stages
 * via one synchronized channel
 */
public class Demultiplexer extends BaseStage<DuplicableStage> {

	private final List<DuplicableStage> nextPipedStages;
	private final int sourceStageDuplication;

	/**
	 * Creates an {@code Demultiplexer} with a given fixed number of piped stages.
	 * @param sourceStageDuplication the number of stage duplication
	 */
	private Demultiplexer(BlockingQueue<Statement> sourceChannel, int sourceStageDuplication) {

		super(sourceChannel);

		if (sourceStageDuplication <= 0) {
			throw new IllegalArgumentException("next stages duplication should be greater than 0: count="+sourceStageDuplication);
		}
		this.sourceStageDuplication = sourceStageDuplication;
		this.nextPipedStages = new ArrayList<>();
	}

	public static Demultiplexer fromStage(DuplicableStage stage, int sourceStageDuplication) {

		if (stage.getSourceChannel() == null) {
			throw new IllegalArgumentException("stage " +stage.getName()+" is not piped to a source channel");
		}

		BlockingQueue<Statement> sourceChannel = stage.getSourceChannel();

		if (sourceChannel.remainingCapacity() <= 0) {
			throw new IllegalArgumentException("stage source channel capacity should be greater than 0: capacity="+sourceChannel.remainingCapacity());
		}

		if (sourceStageDuplication <= 0) {
			throw new IllegalArgumentException("duplication should be greater than 0: duplication="+sourceStageDuplication);
		}

		return new Demultiplexer(new ArrayBlockingQueue<>(sourceChannel.remainingCapacity() * sourceStageDuplication), sourceStageDuplication);
	}

	/**
	 * Pipe this Demux to n duplicated stages chain
	 *
	 * @param headStage a duplicable head of the chain's stage
	 */
	@Override
	public void pipe(DuplicableStage headStage) {

		pipe(new DuplicableStageChain(headStage));
	}

	void pipe(DuplicableStageChain originalChain) {

		List<DuplicableStageChain> chains = new ArrayList<>();
		chains.add(originalChain);

		if (getSinkChannel() == null) {

			throw new IllegalStateException("Demux stage not piped: cannot pipe to next stage "+originalChain.getHead().getName());
		}

		if (sourceStageDuplication > 1) {
			chains.addAll(originalChain.duplicateNTimes(getSinkChannel().remainingCapacity(), sourceStageDuplication - 1));
		}

		pipe(chains);
	}

	private void pipe(List<DuplicableStageChain> chains) {

		if (!nextPipedStages.isEmpty()) {
			unpipe();
		}

		for (DuplicableStageChain chain : chains) {

			chain.getHead().setSinkChannel(getSourceChannel());
			nextPipedStages.add(chain.getHead());
		}
	}

	@Override
	public void unpipe() {

		for (DuplicableStage nextPipedStage : nextPipedStages) {

			nextPipedStage.setSinkChannel(null);
		}

		nextPipedStages.clear();
	}

	@Override
	public RunnableStage newRunnableStage() {

		return new RunnableStage(this);
	}

	@Override
	protected ElementEventHandler createElementEventHandler() throws FileNotFoundException {

		return new BaseStage.ElementLog(getName());
	}

	@Override
	public Stream<DuplicableStage> getPipedStages() {

		return nextPipedStages.stream();
	}

	@Override
	public DuplicableStage getFirstPipedStage() {

		throw new IllegalStateException("should not call nextFirstSink() in Demultiplexer");
	}

	private static class RunnableStage extends BaseRunnableStage<Demultiplexer> {

		private int poisonedStatementReceived = 0;

		public RunnableStage(Demultiplexer demultiplexer) {

			super(demultiplexer);
		}

		@Override
		public boolean handleFlow() throws Exception {

			Demultiplexer demultiplexer = getStage();

			Statement current = demultiplexer.getSinkChannel().take();

			BlockingQueue<Statement> sourceChannel = demultiplexer.getSourceChannel();

			sourceChannel.put(current);

			((FlowLog)getFlowEventHandler()).statementHandled(current, demultiplexer.getSinkChannel(), sourceChannel);

			if (current == POISONED_STATEMENT) {
				poisonedStatementReceived++;
			}

			return poisonedStatementReceived == demultiplexer.countPipedStages();
		}

		@Override
		protected FlowEventHandler createFlowEventHandler() throws FileNotFoundException {

			return new FlowLog(Thread.currentThread().getName());
		}
	}

	private static class FlowLog extends BaseFlowLog {

		private FlowLog(String threadName) throws FileNotFoundException {

			super(threadName);
		}

		@Override
		public void beginOfFlow() {

			sendMessage("start distribution");
		}

		private void statementHandled(Statement statement, BlockingQueue<Statement> sinkChannel,
		                             BlockingQueue<Statement> sourceChannel) {

			statementHandled("distributing", statement, sinkChannel, sourceChannel);
		}

		@Override
		public void endOfFlow() {

			sendMessage(getStatementCount()+" healthy statements distributed");
		}
	}

}