package org.nextprot.pipeline.statement;


import org.junit.Test;
import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.elements.filter.NarcolepticFilter;
import org.nextprot.pipeline.statement.elements.filter.NxFlatRawTableFilter;
import org.nextprot.pipeline.statement.elements.sink.NxFlatMappedTableSink;
import org.nextprot.pipeline.statement.elements.source.Pump;
import org.nextprot.pipeline.statement.elements.source.pump.WebStatementPump;

import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class PipelineBuilderTest {

	// 250" for 100ms naps
	@Test
	public void testPipeline() throws IOException {

		URL url = new URL("http://kant.sib.swiss:9001/glyconnect/2019-01-22/all-entries.json");
		Pump<Statement> pump = new WebStatementPump(url, 100);

		Timer timer = new Timer();

		Pipeline pipeline = new PipelineBuilder()
				.start(timer)
				.source(pump)
				.filter(NxFlatRawTableFilter::new)
				.filter(NarcolepticFilter::new)
				.sink(NxFlatMappedTableSink::new)
				.build();

		pipeline.openValves();

		// Wait for the pipe to complete
		try {
			pipeline.waitForThePipesToComplete();
		} catch (InterruptedException e) {
			System.err.println("pipeline error: "+e.getMessage());
		}
		System.out.println("Done in "+timer.getElapsedTimeInMs() + " ms.");
	}

	// 25" for 100ms naps
	@Test
	public void testPipelineWithDemux() throws IOException {

		URL url = new URL("http://kant.sib.swiss:9001/glyconnect/2019-01-22/all-entries.json");
		Pump<Statement> pump = new WebStatementPump(url, 500);

		Timer timer = new Timer();

		Pipeline pipeline = new PipelineBuilder()
				.start(timer)
				.source(pump)
				//.demuxFilter(c -> new NxFlatRawTableFilter(100), 10)
				.demuxFilter(NarcolepticFilter::new, 2)
				.sink(NxFlatMappedTableSink::new)
				.build();

		pipeline.openValves();

		// Wait for the pipe to complete
		try {
			pipeline.waitForThePipesToComplete();
		} catch (InterruptedException e) {
			System.err.println("pipeline error: "+e.getMessage());
		}
		System.out.println("Done in "+timer.getElapsedTimeInMs() + " ms.");
	}

	private static class Timer implements Pipeline.Monitorable {

		private Instant start;
		private final Map<String, Long> infos;

		public Timer() {
			this.infos = new HashMap<>();
		}

		@Override
		public void started() {

			start = Instant.now();
		}

		@Override
		public void ended() {

			infos.put("elapsed", Duration.between(start, Instant.now()).toMillis());
		}

		public long getElapsedTimeInMs() {

			return infos.get("elapsed");
		}
	}
}