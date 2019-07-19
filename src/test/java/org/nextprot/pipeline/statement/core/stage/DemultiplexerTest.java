package org.nextprot.pipeline.statement.core.stage;

import org.junit.Assert;
import org.junit.Test;


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
	public void newDemuxShouldHaveNSourceChannels() {

		Demultiplexer demux = new Demultiplexer(10, 2);

		Assert.assertEquals(2, demux.countSourceChannels());
	}

	@Test
	public void newDemuxShouldHaveASourceChannelWithCapacity() {

		Demultiplexer demux = new Demultiplexer(10, 2);

		Assert.assertEquals(10, demux.getSourceChannel().remainingCapacity());
	}

	@Test
	public void newCustomDemuxShouldHaveASourceChannelWithHalfCapacity() {

		Demultiplexer demux = new Demultiplexer(10, 2, c -> c/2);

		Assert.assertEquals(5, demux.getSourceChannel().remainingCapacity());
	}

	//@Test
	public void unpipedDemuxShouldCreatesSourceChannels() {

		Demultiplexer demux = new Demultiplexer(10, 2);

		Assert.assertNull(demux.getSinkChannel());
		Assert.assertNull(demux.getSourceChannel());
		Assert.assertEquals(2, demux.countSourceChannels());
		Assert.assertEquals(2, demux.countSourceChannels());
		Assert.assertEquals(2, demux.countSourceChannels());
		Assert.assertEquals(0, demux.countPipedStages());

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