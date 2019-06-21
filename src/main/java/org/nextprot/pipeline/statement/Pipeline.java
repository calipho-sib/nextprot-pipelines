package org.nextprot.pipeline.statement;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.elements.Sink;
import org.nextprot.pipeline.statement.elements.source.Pump;
import org.nextprot.pipeline.statement.elements.source.PumpBasedSource;
import org.nextprot.pipeline.statement.elements.demux.DuplicableElement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class Pipeline {

	private PumpBasedSource source;
	private List<Thread> threads;
	private final Monitorable monitorable;

	public Pipeline(DataCollector dataCollector) {

		source = dataCollector.getSource();
		monitorable = dataCollector.getMonitorable();
	}

	public void openValves() {

		threads = new ArrayList<>();

		source.openValves(threads);

		for (Thread thread : threads) {
			System.out.println("[Pipeline] "+thread.getName() + " valves: opened");
		}
		monitorable.started();
	}

	/**
	 * Wait for all threads in the pipe to terminate
	 */
	public void waitForThePipesToComplete() throws InterruptedException, IOException {

		for (Thread thread : threads) {
			thread.join();
			System.out.println("[Pipeline] "+thread.getName() + " valves: closed");
		}
		closePipelineValves();

		monitorable.ended();
	}

	private void closePipelineValves() throws IOException {

		PipelineElement element = source;

		do {
			element.closeValves();
			element = element.nextElement();
		}
		while(element != null);
	}

	interface StartStep {

		default SourceStep start() {
			return start(new Deaf());
		}

		SourceStep start(Monitorable monitorable);
	}

	interface SourceStep {

		FilterStep source(Pump<Statement> pump);
	}

	interface FilterStep {

		FilterStep filter(Function<Integer, DuplicableElement> filterProvider) throws IOException;

		FilterStep demuxFilter(Function<Integer, DuplicableElement> filterProvider, int sourcePipePortCount) throws IOException;

		TerminateStep sink(Supplier<Sink> sinkProvider) throws IOException;
	}

	interface TerminateStep {

		Pipeline build() throws IOException;
	}

	interface Monitorable {

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

		private PumpBasedSource source;
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

		public PumpBasedSource getSource() {
			return source;
		}

		public void setSource(PumpBasedSource source) {
			this.source = source;
		}

		public Monitorable getMonitorable() {
			return monitorable;
		}

		public void setMonitorable(Monitorable monitorable) {
			this.monitorable = monitorable;
		}
	}
}
