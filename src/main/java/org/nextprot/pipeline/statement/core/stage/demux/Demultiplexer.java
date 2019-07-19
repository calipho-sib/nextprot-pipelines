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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.nextprot.pipeline.statement.core.stage.Source.POISONED_STATEMENT;

/**
 * De-multiplexer receives statements via one source stage and distribute them to multiple sink stages
 * via synchronized channels
 *
 * TODO: put common code with BaseStage in a new abstract class
 */
public class Demultiplexer implements Stage<DuplicableStage> {

	private final int sinkChannelCapacity;
	private BlockingQueue<Statement> sinkChannel;
	private final CircularList<BlockingQueue<Statement>> sourceChannels;
	private final List<DuplicableStage> nextPipedStages;
	private final ElementEventHandler eventHandler;

	private final AtomicInteger incrementer = new AtomicInteger (-1);

	public Demultiplexer(int sinkChannelCapacity, int sourceChannelCount) {

		this(sinkChannelCapacity, sourceChannelCount, c -> c);
	}

	/**
	 * @param sinkChannelCapacity the capacity of the sink channel
	 * @param sourceChannelCount the number of source channels
	 * @param newSourceChannelCapacityLambda the lambda that compute the new source channels capacity
	 */
	public Demultiplexer(int sinkChannelCapacity, int sourceChannelCount, Function<Integer, Integer> newSourceChannelCapacityLambda) {

		if (sinkChannelCapacity <= 0) {
			throw new IllegalArgumentException("sink channel capacity should be greater than 0: capacity="+sinkChannelCapacity);
		}
		if (sourceChannelCount <= 0) {
			throw new IllegalArgumentException("source channel count should be greater than 0: count="+sourceChannelCount);
		}

		int sourceChannelCapacity = newSourceChannelCapacityLambda.apply(sinkChannelCapacity);

		if (sourceChannelCapacity <= 0) {
			throw new IllegalArgumentException("source channel capacity should be greater than 0: capacity="+sourceChannelCapacity);
		}

		this.sinkChannelCapacity = sinkChannelCapacity;
		this.sourceChannels = createSourceChannels(sourceChannelCapacity, sourceChannelCount);
		this.nextPipedStages = new ArrayList<>();

		try {
			eventHandler = createElementEventHandler();
		} catch (FileNotFoundException e) {
			throw new IllegalStateException(e);
		}
	}

	private CircularList<BlockingQueue<Statement>> createSourceChannels(int capacity, int channelCount) {

		CircularList<BlockingQueue<Statement>> sources = new CircularList<>();

		for (int i=0 ; i<channelCount ; i++) {

			sources.add(new ArrayBlockingQueue<>(capacity));
		}

		if (sources.isEmpty()) {

			throw new IllegalArgumentException(getName()+": cannot create source channels, capacity="+capacity
					+", channel count="+channelCount);
		}

		return sources;
	}

	/**
	 * Pipe this Demux to n duplicated stages chain from the given head stage to the last sink stage
	 *
	 * @param headStage a duplicable head stage
	 *
	 * State before this method is called:
	 *
	 * 		 --- STAGE_-1 -- DEMUX --- [ HEAD_0   -> FILTER_1   -> FILTER_2   -> .... -> SINK_N   ]
	 *
	 * State after this method is called:
	 *
	 * 		                     ------ [ HEAD_0.1 -> FILTER_1.1 -> FILTER_2.1 -> .... -> SINK_N.1 ]
	 * 		                    | ----- [ HEAD_0.2 -> FILTER_1.2 -> FILTER_2.2 -> .... -> SINK_N.2 ]
	 * 		 --- STAGE_-1 -- DEMUX  --- [ HEAD_0.3 -> FILTER_1.3 -> FILTER_2.3 -> .... -> SINK_N.3 ]
	 * 		                    | ----- [ HEAD_0.4 -> FILTER_1.4 -> FILTER_2.4 -> .... -> SINK_N.4 ]
	 * 		                     ------ [ HEAD_0.5 -> FILTER_1.5 -> FILTER_2.5 -> .... -> SINK_N.5 ]
	 */
	@Override
	public void pipe(DuplicableStage headStage) {

		DuplicableStageChain duplicableStageChain = new DuplicableStageChain(headStage);

		for (int i = 0; i < sourceChannels.size()-1; i++) {

			BlockingQueue<Statement> sourceChannel = sourceChannels.get(i);

			DuplicableStageChain duplicatedChain = duplicableStageChain.duplicate(sourceChannel.remainingCapacity());

			duplicatedChain.getHead().setSinkChannel(sourceChannel);

			nextPipedStages.add(duplicatedChain.getHead());
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

		return sourceChannels.get(incrementer.incrementAndGet());
	}

	public int countSourceChannels() {
		return sourceChannels.size();
	}

	@Override
	public Stream<DuplicableStage> getPipedStages() {

		return nextPipedStages.stream();
	}

	@Override
	public DuplicableStage getFirstPipedStage() {

		throw new IllegalStateException("should not call nextFirstSink() in Demultiplexer");
	}

	private static class CircularList<E> extends ArrayList<E> {

		@Override
		public E get(int index) {
			return super.get(index % size());
		}
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

			return poisonedStatementReceived == demultiplexer.countSourceChannels();
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