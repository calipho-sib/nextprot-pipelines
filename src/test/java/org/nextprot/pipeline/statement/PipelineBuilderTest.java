package org.nextprot.pipeline.statement;


import org.junit.Test;
import org.mockito.Mockito;
import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.elements.NxFlatTableSink;
import org.nextprot.pipeline.statement.elements.Source;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

public class PipelineBuilderTest {

	@Test
	public void testPipeline() throws IOException {

		URL url = new URL("http://kant.sib.swiss:9001/glyconnect/2019-01-22/all-entries.json");
		Reader reader = new InputStreamReader(url.openStream());
		Pump<Statement> pump = new Source.StatementPump(reader, 10);

		Pipeline pipeline = new PipelineBuilder()
				.start()
				.source(pump)
				.filter(NarcolepticFilter::new)
				.filter(NarcolepticFilter::new)
				.sink((c) -> new NxFlatTableSink(NxFlatTableSink.Table.entry_mapped_statements))
				.build();

		pipeline.open();

		// Wait for the pipe to complete
		try {
			pipeline.waitForThePipesToComplete();
		} catch (InterruptedException e) {
			System.err.println("pipeline error: "+e.getMessage());
		}
		System.out.println("Done.");
	}

	@Test
	public void testPipelineWithDemux() throws IOException {

		URL url = new URL("http://kant.sib.swiss:9001/glyconnect/2019-01-22/all-entries.json");
		Reader reader = new InputStreamReader(url.openStream());
		Pump<Statement> pump = new Source.StatementPump(reader, 100);

		Pipeline pipeline = new PipelineBuilder()
				.start(new Timer())
				.source(pump)
				.demuxFromFilter(c -> new NarcolepticFilter(c, 500), 10)
				.sink((c) -> new NxFlatTableSink(NxFlatTableSink.Table.entry_mapped_statements))
				.build();

		pipeline.open();

		// Wait for the pipe to complete
		try {
			pipeline.waitForThePipesToComplete();
		} catch (InterruptedException e) {
			System.err.println("pipeline error: "+e.getMessage());
		}
		System.out.println("Done.");
	}

	public static class Timer implements Pipeline.Monitorable {

		private Instant start;

		@Override
		public void started(List<Thread> threads) {

			start = Instant.now();
		}

		@Override
		public void ended() {

			Instant finish = Instant.now();
			long timeElapsed = Duration.between(start, finish).toMillis();

			System.out.println("ELAPSED TIME: "+ timeElapsed + "ms");
		}
	}
}