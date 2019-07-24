package org.nextprot.pipeline.statement.core.stage.demux;


import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.core.Stage;
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
import java.util.function.Function;
import java.util.stream.Stream;

import static org.nextprot.pipeline.statement.core.stage.Source.POISONED_STATEMENT;

/**
 * De-multiplexer receives statements via one source stage and distribute them to multiple sink stages
 * via one synchronized channel
 *
 * TODO: put common code with BaseStage in a new abstract class
 */
public class Demultiplexer implements Stage<DuplicableStage> {

	private final int sinkChannelCapacity;
	private BlockingQueue<Statement> sinkChannel;
	private final BlockingQueue<Statement> sourceChannel;
	private final List<DuplicableStage> nextPipedStages;
	private final ElementEventHandler eventHandler;
	private final int sourceStageDuplication;

	public Demultiplexer(int sinkChannelCapacity, int sourceStageDuplication) {

		this(sinkChannelCapacity, sourceStageDuplication, c -> c * sourceStageDuplication);
	}

	/**
	 * Creates an {@code Demultiplexer} with a given fixed number of piped stages.
	 * @param sinkChannelCapacity the capacity of the sink channel
	 * @param sourceStageDuplication the number of stage duplication
	 * @param newSourceChannelCapacityLambda the lambda that compute the new source channels capacity
	 */
	public Demultiplexer(int sinkChannelCapacity, int sourceStageDuplication, Function<Integer, Integer> newSourceChannelCapacityLambda) {

		if (sinkChannelCapacity <= 0) {
			throw new IllegalArgumentException("sink channel capacity should be greater than 0: capacity="+sinkChannelCapacity);
		}
		if (sourceStageDuplication <= 0) {
			throw new IllegalArgumentException("next stages duplication should be greater than 0: count="+sourceStageDuplication);
		}

		int sourceChannelCapacity = newSourceChannelCapacityLambda.apply(sinkChannelCapacity);

		if (sourceChannelCapacity <= 0) {
			throw new IllegalArgumentException("source channel capacity should be greater than 0: capacity="+sourceChannelCapacity);
		}

		this.sinkChannelCapacity = sinkChannelCapacity;
		this.nextPipedStages = new ArrayList<>();
		this.sourceChannel = new ArrayBlockingQueue<>(sourceChannelCapacity);
		this.sourceStageDuplication = sourceStageDuplication;

		try {
			eventHandler = createElementEventHandler();
		} catch (FileNotFoundException e) {
			throw new IllegalStateException(e);
		}
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

		if (sourceStageDuplication > 1) {
			chains.addAll(originalChain.duplicateNTimes(sinkChannelCapacity, sourceStageDuplication - 1));
		}

		pipe(chains);
	}

	private void pipe(List<DuplicableStageChain> chains) {

		for (int i = 0; i < chains.size(); i++) {

			DuplicableStageChain chain = chains.get(i);

			chain.getHead().setSinkChannel(sourceChannel);
			nextPipedStages.add(chain.getHead());
		}
	}

	@Override
	public void close() {

		eventHandler.sinkUnpiped();
	}

	@Override
	public RunnableStage newRunnableStage() {

		return new RunnableStage(this);
	}

	private ElementEventHandler createElementEventHandler() throws FileNotFoundException {

		//return new ElementEventHandler.Mute();
		return new BaseStage.ElementLog(getName());
	}

	public String getName() {
		return "Demux";
	}

	@Override
	public BlockingQueue<Statement> getSinkChannel() {
		return sinkChannel;
	}

	@Override
	public void setSinkChannel(BlockingQueue<Statement> channel) {

		if (sinkChannelCapacity != channel.remainingCapacity()) {

			throw new Error("Cannot set sink channel with capacity "+channel.remainingCapacity() + " in channel port of capacity "+ sinkChannel.remainingCapacity());
		}

		this.sinkChannel = channel;
	}

	/** In this context, only one current source channel is active */
	@Override
	public BlockingQueue<Statement> getSourceChannel() {

		return sourceChannel;
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

			((FlowLog)getFlowEventHandler()).statementHandled(current, demultiplexer.sinkChannel, sourceChannel);

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