package org.nextprot.pipeline.statement;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.elements.Sink;
import org.nextprot.pipeline.statement.elements.Source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class Pipeline {

	private Source source;
	private List<Thread> threads;
	private final Monitorable monitorable;

	public Pipeline(DataCollector dataCollector) {

		source = dataCollector.getSource();
		monitorable = dataCollector.getMonitorable();
		//Demultiplexer demultiplexer = new Demultiplexer(source.getCapacity(), source.getSinkPipe(), 2);
		//demultiplexer.connect();
	}

	public void open() {

		threads = new ArrayList<>();

		source.start(threads);
		monitorable.started(threads);
	}

	/**
	 * Wait for all threads in the pipe to terminate
	 */
	public void waitForThePipesToComplete() throws InterruptedException {

		for (Thread thread : threads) {
			thread.join();
			System.out.println("Pipe " + thread.getName() + ": closed");
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

		FilterStep filter(Function<Integer, Filter> filterProvider) throws IOException;

		TerminateStep sink(Function<Integer, Sink> sinkProvider) throws IOException;
	}

	interface TerminateStep {

		Pipeline build() throws IOException;
	}

	interface Monitorable {

		void started(List<Thread> threads);

		void ended();
	}

	static class Deaf implements Monitorable {

		@Override
		public void started(List<Thread> threads) { }

		@Override
		public void ended() { }
	}

	static class DataCollector {

		private Source source;
		private Monitorable monitorable;

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
