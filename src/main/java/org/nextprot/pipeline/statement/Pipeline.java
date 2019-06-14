package org.nextprot.pipeline.statement;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.elements.Sink;
import org.nextprot.pipeline.statement.elements.Source;
import org.nextprot.pipeline.statement.muxdemux.DuplicableElement;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class Pipeline {

	private Source source;
	private List<Thread> threads;
	private final Monitorable monitorable;

	public Pipeline(DataCollector dataCollector) {

		source = dataCollector.getSource();
		monitorable = dataCollector.getMonitorable();
	}

	public void open() {

		threads = new ArrayList<>();

		source.run(threads);

		for (Thread thread : threads) {
			System.out.println("Thread "+thread.getName() + ": created");
		}
		monitorable.started();
	}

	/**
	 * Wait for all threads in the pipe to terminate
	 */
	public void waitForThePipesToComplete() throws InterruptedException {

		for (Thread thread : threads) {
			thread.join();
			System.out.println("Thread "+thread.getName() + ": died");
		}
		monitorable.ended();
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
}
