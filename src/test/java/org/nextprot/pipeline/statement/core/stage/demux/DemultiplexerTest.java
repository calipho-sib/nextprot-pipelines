package org.nextprot.pipeline.statement.core.stage.demux;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.nextprot.pipeline.statement.core.stage.DuplicableStage;

import java.util.ArrayList;
import java.util.List;


public class DemultiplexerTest {

	@Test(expected = IllegalArgumentException.class)
	public void sinkChannelCapacityShouldBeGreaterThanZero() {

		new Demultiplexer(0, 1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void sourceChannelCountShouldBeGreaterThanZero() {

		new Demultiplexer(10, 0);
	}

	@Test
	public void newDemuxShouldNotHaveSinkChannel() {

		Demultiplexer demux = new Demultiplexer(10, 2);

		Assert.assertNull(demux.getSinkChannel());
	}

	@Test
	public void newDemuxShouldHaveNoPipedStages() {

		Demultiplexer demux = new Demultiplexer(10, 2);
		Assert.assertEquals(0, demux.countPipedStages());
	}

	@Test
	public void newDemuxShouldHaveASourceChannelWithDoubleCapacity() {

		Demultiplexer demux = new Demultiplexer(10, 2);
		Assert.assertEquals(20, demux.getSourceChannel().remainingCapacity());
	}

	@Test
	public void newCustomDemuxShouldHaveASourceChannelWithHalfCapacity() {

		Demultiplexer demux = new Demultiplexer(10, 2, c -> c/2);

		Assert.assertEquals(5, demux.getSourceChannel().remainingCapacity());
	}

	@Test
	public void unpipedDemuxShouldNotBeConnectedToNextStages() {

		Demultiplexer demux = new Demultiplexer(10, 2);

		Assert.assertEquals(0, demux.countPipedStages());
	}

	@Test
	public void pipedDemuxShouldConnectToNextStages() {

		Demultiplexer demux = new Demultiplexer(10, 2);

		demux.pipe(mockDuplicableStageChain(10, 1));

		Assert.assertEquals(2, demux.countPipedStages());
	}

	@Test
	public void pipedTwiceDemuxShouldConnectToNextStages() {

		Demultiplexer demux = new Demultiplexer(10, 2);

		demux.pipe(mockDuplicableStageChain(10, 1));
		demux.pipe(mockDuplicableStageChain(10, 1));

		Assert.assertEquals(2, demux.countPipedStages());
	}

	@Test
	public void unpipeDemux() {

		Demultiplexer demux = new Demultiplexer(10, 2);

		demux.pipe(mockDuplicableStageChain(10, 1));
		demux.unpipe();

		Assert.assertEquals(0, demux.countPipedStages());
	}

	private DuplicableStageChain mockDuplicableStageChain(int capacity, int duplication) {

		DuplicableStage stage = Mockito.mock(DuplicableStage.class);

		DuplicableStageChain chain = Mockito.mock(DuplicableStageChain.class);
		Mockito.when(chain.getHead()).thenReturn(stage);

		List<DuplicableStageChain> chains = new ArrayList<>();

		for (int i=0 ; i<duplication ; i++) {
			chains.add(chain);
		}

		Mockito.when(chain.duplicateNTimes(capacity, duplication)).thenReturn(chains);

		return chain;
	}

	/*@Test
	public void connect() throws IOException, InterruptedException {

		URL url = new URL("http://kant.sib.swiss:9001/glyconnect/2019-01-22/all-entries.json");
		Reader reader = new InputStreamReader(url.openStream());
		Pump<Statement> pump = new Source.StatementPump(reader, 10);

		Source source = new Source(pump);
		Demultiplexer demux = new Demultiplexer(new SinkPipePort(10), 2);
		Sink sink = new NxFlatTableSink(NxFlatTableSink.Table.entry_mapped_statements);
		Filter filter = new NarcolepticFilter(10);

		filter.pipe(sink);


		//   SRC(10) --> Demux(10) --> Filter(5) --> Sink(5)
	    //    	             \_____  Filter(5) --> Sink(5)

		//source.pipe(demux);
		demux.pipe(filter);

		List<Thread> tl = new ArrayList<>();
		demux.start(tl);

		for (Thread thread : tl) {
			thread.join();
			System.out.println("Pipe " + thread.getName() + ": closed");
		}
		System.out.println(tl);
	}*/
}