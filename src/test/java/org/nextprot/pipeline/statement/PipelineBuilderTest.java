package org.nextprot.pipeline.statement;


import org.junit.Test;
import org.nextprot.pipeline.statement.elements.NarcolepticFilter;
import org.nextprot.pipeline.statement.elements.NxFlatTableSink;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class PipelineBuilderTest {

	// 250" for 100ms naps
	@Test
	public void testPipeline() throws IOException {

		String url = "http://kant.sib.swiss:9001/glyconnect/2019-01-22/all-entries.json";

		Timer timer = new Timer();

		Pipeline pipeline = new PipelineBuilder()
				.start(timer)
				.source(url, 5000)
				.filter(c -> new NarcolepticFilter(c, 100))
				.sink(c -> new NxFlatTableSink(NxFlatTableSink.Table.entry_mapped_statements))
				.build();

		pipeline.open();

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

		// TRY TO DISABLE THE LOGGING!!!!

		URL url = new URL("http://kant.sib.swiss:9001/glyconnect/2019-01-22/all-entries.json");
		Reader reader = new InputStreamReader(url.openStream());

		Timer timer = new Timer();

		Pipeline pipeline = new PipelineBuilder()
				.start(timer)
				.source(reader, 100)
				.demuxFilter(c -> new NarcolepticFilter(c, 100), 10)
				.sink(c -> new NxFlatTableSink(NxFlatTableSink.Table.entry_mapped_statements))
				.build();

		pipeline.open();

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