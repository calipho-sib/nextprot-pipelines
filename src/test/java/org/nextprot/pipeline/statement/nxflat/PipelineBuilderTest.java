package org.nextprot.pipeline.statement.nxflat;


import org.junit.Test;
import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.core.Pipeline;
import org.nextprot.pipeline.statement.core.PipelineBuilder;
import org.nextprot.pipeline.statement.core.elements.filter.NarcolepticFilter;
import org.nextprot.pipeline.statement.nxflat.filter.NxFlatRawTableFilter;
import org.nextprot.pipeline.statement.nxflat.sink.NxFlatMappedTableSink;
import org.nextprot.pipeline.statement.core.elements.source.Pump;
import org.nextprot.pipeline.statement.nxflat.source.pump.WebStatementPump;

import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PipelineBuilderTest {

	// 250" for 100ms naps
	@Test
	public void testPipeline() throws Exception {

		URL url = new URL("http://kant.sib.swiss:9001/glyconnect/2019-01-22/all-entries.json");

		Timer timer = new Timer();
		Pipeline pipeline = newPipeline(url, 5000, 1, 1, timer);

		pipeline.openValves();

		// Wait for the pipe to complete
		try {
			pipeline.waitForThePipesToComplete();
		} catch (InterruptedException e) {
			System.err.println("pipeline error: " + e.getMessage());
		}

		System.out.println("Done in "+timer.getElapsedTimeInMs() + " ms.");
	}

	@Test
	public void benchmarkPipelinesVariateCapacities() throws Exception {

		URL url = new URL("http://kant.sib.swiss:9001/glyconnect/2019-01-22/all-entries.json");

		List<int[]> capacityAndSplitList = new ArrayList<>();

		capacityAndSplitList.add(new int[] {100, 100});
		capacityAndSplitList.add(new int[] {200, 100});
		capacityAndSplitList.add(new int[] {500, 100});
		capacityAndSplitList.add(new int[] {750, 100});
		capacityAndSplitList.add(new int[] {1000, 100});
		capacityAndSplitList.add(new int[] {2500, 100});
		capacityAndSplitList.add(new int[] {5000, 100});

		System.out.println(benchmarking(url, capacityAndSplitList, 100));

		/*
		 * [100,  100]=2612 ms
		 * [200,  100]=2562 ms
		 * [500,  100]=2590 ms
		 * [750,  100]=2587 ms
		 * [1000, 100]=2579 ms
		 * [2500, 100]=2581 ms
		 * [5000, 100]=2583 ms
		 */
		// Interpretation: capacity does not affect the overall pipeline duration
	}

	// 25" for 100ms naps
	// 83" for 1" nap (30 threads) instead of 2500"
	@Test
	public void benchmarkPipelines() throws Exception {

		URL url = new URL("http://kant.sib.swiss:9001/glyconnect/2019-01-22/all-entries.json");

		List<int[]> capacityAndSplitList = new ArrayList<>();

		capacityAndSplitList.add(new int[] {200, 1});
		capacityAndSplitList.add(new int[] {200, 5});
		capacityAndSplitList.add(new int[] {200, 10});
		capacityAndSplitList.add(new int[] {200, 50});
		capacityAndSplitList.add(new int[] {200, 100});
		capacityAndSplitList.add(new int[] {200, 200});

		System.out.println(benchmarking(url, capacityAndSplitList, 100));

		/*
		 * [200, 1  ]=244s
		 * [200, 5  ]= 49s
		 * [200, 10 ]= 24s
		 * [200, 50 ]=  5s
		 * [200, 100]=2.6s
		 * [200, 200]=1.4s
		 */
		// Interpretation: capacity does not affect the overall pipeline duration
	}

	private Pipeline newPipeline(URL url, int capacity, int split, long nap, Timer timer) throws Exception {

		if (split > capacity) {

			throw new IllegalStateException("indivisible splits: capacity=" + capacity + ", splits=" + split);
		}

		Pump<Statement> pump = new WebStatementPump(url, capacity);

		Pipeline.FilterStep filterStep = new PipelineBuilder()
				.start(timer)
				.source(pump);

		if (split == 1) {
			filterStep = filterStep.filter(NxFlatRawTableFilter::new);
		}
		else {
			filterStep = filterStep.split(NxFlatRawTableFilter::new, split);
		}

		return filterStep
				.filter(c -> new NarcolepticFilter(c, nap))
				.sink(NxFlatMappedTableSink::new)
				.build();
	}

	private Map<String, Long> benchmarking(URL url, List<int[]> capacityAndSplitList, long nap) throws Exception {

		Map<String, Long> durations = new HashMap<>();

		for (int[] capacityAndSplit : capacityAndSplitList) {

			System.err.println("processing: capacity=" + capacityAndSplit[0] + ", splits=" + capacityAndSplit[1]);

			Timer timer = new Timer();
			Pipeline pipeline = newPipeline(url, capacityAndSplit[0], capacityAndSplit[1], nap, timer);

			pipeline.openValves();

			// Wait for the pipe to complete
			try {
				pipeline.waitForThePipesToComplete();
			} catch (InterruptedException e) {
				System.err.println("pipeline error: " + e.getMessage());
			}
			durations.put(Arrays.toString(capacityAndSplit), timer.getElapsedTimeInMs());
		}

		return durations;
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