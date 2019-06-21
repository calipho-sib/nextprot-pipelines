package org.nextprot.pipeline.statement.core;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.core.elements.Sink;
import org.nextprot.pipeline.statement.core.elements.Source;
import org.nextprot.pipeline.statement.core.elements.source.Pump;
import org.nextprot.pipeline.statement.core.elements.demux.DuplicableElement;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class Pipeline {

	private Source source;
	private List<Thread> threads;
	private final Monitorable monitorable;
	private final Log log;

	public Pipeline(DataCollector dataCollector) throws FileNotFoundException {

		source = dataCollector.getSource();
		monitorable = dataCollector.getMonitorable();

		log = new Log();
	}

	public void openValves() {

		threads = new ArrayList<>();

		source.openValves(threads);

		for (Thread thread : threads) {
			log.valvesOpened(thread);
		}
		monitorable.started();
	}

	/**
	 * Wait for all threads in the pipe to terminate
	 */
	public void waitForThePipesToComplete() throws InterruptedException, IOException {

		for (Thread thread : threads) {
			thread.join();
			log.valvesClosed(thread);
		}
		closePipelineValves();

		monitorable.ended();
	}

	private void closePipelineValves() throws IOException {

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

		FilterStep source(Pump<Statement> pump);
	}

	public interface FilterStep {

		FilterStep filter(Function<Integer, DuplicableElement> filterProvider) throws IOException;

		FilterStep demuxFilter(Function<Integer, DuplicableElement> filterProvider, int sourcePipePortCount) throws IOException;

		TerminateStep sink(Supplier<Sink> sinkProvider) throws IOException;
	}

	public interface TerminateStep {

		Pipeline build() throws Exception;
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
		private int demuxSourcePipePortCount;
		private PipelineElement elementBeforeDemux;
		private DuplicableElement fromElement;

		public int getDemuxSourcePipePortCount() {
			return demuxSourcePipePortCount;
		}

		public DuplicableElement getDemuxFromElement() {
			return fromElement;
		}

		public PipelineElement getElementBeforeDemux() {
			return elementBeforeDemux;
		}

		public void setDemuxSourcePipePortCount(int sourcePipePortCount) {
			this.demuxSourcePipePortCount = sourcePipePortCount;
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

	public static class Log extends BaseLog {

		public Log() throws FileNotFoundException {

			super("Pipeline");
		}

		public void valvesOpened(Thread thread) {

			sendMessage(thread.getName() + " valves: opened");
		}

		public void valvesClosed(Thread thread) {

			sendMessage(thread.getName() + " valves: closed");
		}
	}}
