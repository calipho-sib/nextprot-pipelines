package org.nextprot.pipeline.statement.core;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.core.elements.Sink;
import org.nextprot.pipeline.statement.core.elements.Source;
import org.nextprot.pipeline.statement.core.elements.filter.BaseFilter;
import org.nextprot.pipeline.statement.core.elements.flowable.Valve;
import org.nextprot.pipeline.statement.core.elements.source.Pump;
import org.nextprot.pipeline.statement.core.elements.demux.DuplicableElement;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class Pipeline {

	private static int FLOWABLE_NUMBER = 0;

	private static synchronized int NEXT_FLOWABLE_NUM() {
		return FLOWABLE_NUMBER++;
	}

	private final Source source;
	private final List<Thread> activeValves;
	private final Monitorable monitorable;
	private final Log log;

	public Pipeline(DataCollector dataCollector) throws FileNotFoundException {

		source = dataCollector.getSource();
		monitorable = dataCollector.getMonitorable();

		log = new Log();
		activeValves = new ArrayList<>();
	}

	public void openValves() {

		List<Valve> valves = new ArrayList<>();

		source.openValves(valves);

		for (Valve valve : valves) {

			Thread activeValve = newThread(valve);
			activeValve.start();
			activeValves.add(activeValve);

			log.valvesOpened(activeValve);
		}
		monitorable.started();
	}

	private Thread newThread(Valve valve) {

		Thread thread = new Thread(valve);
		thread.setName(valve.getStage().getName()+ "-" + NEXT_FLOWABLE_NUM());

		return thread;
	}

	/**
	 * Wait for all valves in the pipeline to terminate controlling flow
	 */
	public void waitUntilCompletion() throws InterruptedException {

		for (Thread activeValve : activeValves) {
			activeValve.join();
			log.valvesClosed(activeValve);
		}
		closeValves();
		monitorable.ended();
	}

	private void closeValves() {

		PipelineElement element = source;

		do {
			element.closeValves();
			element = element.nextSink();
		}
		while(element != null);
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

		FilterStep filter(Function<Integer, DuplicableElement> filterProvider);
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
		private int demuxSourceCount;
		private PipelineElement elementBeforeDemux;
		private DuplicableElement fromElement;

		public int getDemuxSourceCount() {
			return demuxSourceCount;
		}

		public DuplicableElement getDemuxFromElement() {
			return fromElement;
		}

		public PipelineElement getElementBeforeDemux() {
			return elementBeforeDemux;
		}

		public void setDemuxSourceCount(int demuxSourceCount) {
			this.demuxSourceCount = demuxSourceCount;
		}

		public void setDemuxFromElement(PipelineElement elementBeforeDemux, DuplicableElement fromElement) {
			this.elementBeforeDemux = elementBeforeDemux;
			this.fromElement = fromElement;
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

		private void valvesOpened(Thread thread) {

			sendMessage(thread.getName() + " valves: opened");
		}

		private void valvesClosed(Thread thread) {

			sendMessage(thread.getName() + " valves: closed");
		}
	}
}
