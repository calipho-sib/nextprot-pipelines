package org.nextprot.pipeline.statement.core;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.core.stage.Sink;
import org.nextprot.pipeline.statement.core.stage.Source;
import org.nextprot.pipeline.statement.core.stage.filter.BaseFilter;
import org.nextprot.pipeline.statement.core.stage.runnable.RunnableStage;
import org.nextprot.pipeline.statement.core.stage.source.Pump;
import org.nextprot.pipeline.statement.core.stage.DuplicableStage;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class Pipeline {

	private static int ACTIVE_STAGE_NUMBER = 0;

	private static synchronized int NEXT_ACTIVE_STAGE_NUMBER() {
		return ACTIVE_STAGE_NUMBER++;
	}

	private final Source source;
	private final List<Thread> activeStages;
	private final Monitorable monitorable;
	private final Log log;

	public Pipeline(DataCollector dataCollector) throws FileNotFoundException {

		source = dataCollector.getSource();
		monitorable = dataCollector.getMonitorable();

		log = new Log();
		activeStages = new ArrayList<>();
	}

	public void openValves() {

		List<RunnableStage> runnableStages = new ArrayList<>();

		openValves(source, runnableStages);

		for (RunnableStage runnableStage : runnableStages) {

			Thread activeStage = newThread(runnableStage);
			activeStage.start();
			activeStages.add(activeStage);

			log.valvesOpened(activeStage);
		}
		monitorable.started();
	}

	private void openValves(Stage stage, List<RunnableStage> runningValves) {

		RunnableStage runnableStage = stage.newRunnableStage();
		log.valveOpened(runnableStage);
		runningValves.add(runnableStage);

		Stream<Stage> nextStages = stage.getPipedStages();
		nextStages.forEach(s -> openValves(s, runningValves));
	}

	private void closeValves(Stage stage) {

		stage.close();

		Stream<Stage> nextStages = stage.getPipedStages();
		nextStages.forEach(sink -> closeValves(sink));
	}

	private Thread newThread(RunnableStage runnableStage) {

		Thread thread = new Thread(runnableStage);
		thread.setName(runnableStage.getStage().getName()+ "-" + NEXT_ACTIVE_STAGE_NUMBER());

		return thread;
	}

	/**
	 * Wait for all valves in the pipeline to terminate controlling flow
	 */
	public void waitUntilCompletion() throws InterruptedException {

		for (Thread activeStage : activeStages) {
			activeStage.join();
			log.valvesClosed(activeStage);
		}
		closeValves(source);
		monitorable.ended();
	}

	interface StartStep {

		default SourceStep start() {
			return start(new Deaf());
		}

		SourceStep start(Monitorable monitorable);
	}

	public interface SourceStep {

		FilterStep source(Pump<Statement> pump, int capacity);
	}

	public interface FilterStep {

		FilterStep filter(Function<Integer, DuplicableStage> filterProvider);
		TerminateStep sink(Supplier<Sink> sinkProvider);

		FilterStep split(Function<Integer, BaseFilter> filterProvider, int partitionCount);
		TerminateStep split(Supplier<Sink> SinkProvider, int partitionCount);
	}

	public interface TerminateStep {

		Pipeline build();
	}

	public interface Monitorable {

		void started();

		void ended();
	}

	static class Deaf implements Monitorable {

		@Override
		public void started() { }

		@Override
		public void ended() { }
	}

	static class DataCollector {

		private Source source;
		private Monitorable monitorable;
		private int demuxStageDuplication;
		private Stage stageBeforeDemux;
		private DuplicableStage fromStage;

		public int getDemuxStageDuplication() {
			return demuxStageDuplication;
		}

		public DuplicableStage getDemuxFromStage() {
			return fromStage;
		}

		public Stage getStageBeforeDemux() {
			return stageBeforeDemux;
		}

		public void setDemuxStageDuplication(int demuxStageDuplication) {
			this.demuxStageDuplication = demuxStageDuplication;
		}

		public void setDemuxFromStage(Stage stageBeforeDemux, DuplicableStage fromStage) {
			this.stageBeforeDemux = stageBeforeDemux;
			this.fromStage = fromStage;
		}

		public Source getSource() {
			return source;
		}

		public void setSource(Source source) {
			this.source = source;
		}

		public Monitorable getMonitorable() {
			return monitorable;
		}

		public void setMonitorable(Monitorable monitorable) {
			this.monitorable = monitorable;
		}
	}

	private static class Log extends BaseLog {

		private Log() throws FileNotFoundException {

			super("Pipeline");
		}

		private void valveOpened(RunnableStage stage) {

			sendMessage(stage.getStage().getName() + " valve: opened");
		}

		private void valveClosed(RunnableStage stage) {

			sendMessage(stage.getStage().getName() + " valve: closed");
		}

		private void valvesOpened(Thread thread) {

			sendMessage(thread.getName() + " valves: opened");
		}

		private void valvesClosed(Thread thread) {

			sendMessage(thread.getName() + " valves: closed");
		}
	}
}
