package org.nextprot.pipeline.statement.core.stage.demux;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.core.stage.DuplicableStage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;


public class DemultiplexerTest {

	@Test(expected = IllegalArgumentException.class)
	public void stageShouldBePiped() {

		Demultiplexer.fromStage(mockDuplicableStage(null),  10);
	}

	@Test(expected = IllegalArgumentException.class)
	public void duplicationShouldBeGreaterThanZero() {

		Demultiplexer.fromStage(mockDuplicableStage(mockBlockingQueue(10)),  0);
	}

	@Test(expected = IllegalArgumentException.class)
	public void stageSourceCapacityShouldBeGreaterThanZero() {

		Demultiplexer.fromStage(mockDuplicableStage(mockBlockingQueue(0)),  0);
	}

	@Test
	public void newDemuxSourceChannelShouldHaveLargerCapacity() {

		Demultiplexer demux = Demultiplexer.fromStage(mockDuplicableStage(mockBlockingQueue(10)),  2);
		Assert.assertEquals(20, demux.getSourceChannel().remainingCapacity());
	}

	@Test
	public void newDemuxShouldNotHaveSinkChannel() {

		Demultiplexer demux = Demultiplexer.fromStage(mockDuplicableStage(mockBlockingQueue(10)),  2);
		Assert.assertNull(demux.getSinkChannel());
	}

	@Test
	public void newDemuxShouldNotBeConnectedToNextStages() {

		Demultiplexer demux = Demultiplexer.fromStage(mockDuplicableStage(mockBlockingQueue(10)),  2);
		Assert.assertEquals(0, demux.countPipedStages());
	}

	@Test(expected = IllegalStateException.class)
	public void unpipedDemuxCannotPipeToNextStages() {

		Demultiplexer demux = Demultiplexer.fromStage(mockDuplicableStage(mockBlockingQueue(10)),  2);
		demux.pipe(mockDuplicableStageChain( 1));
	}

	@Test
	public void pipedDemuxCanPipeToNextStages() {

		Demultiplexer demux = Demultiplexer.fromStage(mockDuplicableStage(mockBlockingQueue(10)),  2);
		demux.setSinkChannel(mockBlockingQueue(10));

		demux.pipe(mockDuplicableStageChain( 1));

		Assert.assertEquals(2, demux.countPipedStages());
	}

	@Test
	public void alreadyPipedDemuxCanPipeToBrandNewNextStages() {

		Demultiplexer demux = Demultiplexer.fromStage(mockDuplicableStage(mockBlockingQueue(10)),  2);
		demux.setSinkChannel(mockBlockingQueue(10));

		demux.pipe(mockDuplicableStageChain( 1));
		demux.pipe(mockDuplicableStageChain( 1));

		Assert.assertEquals(2, demux.countPipedStages());
	}

	@Test
	public void unpipeDemux() {

		Demultiplexer demux = Demultiplexer.fromStage(mockDuplicableStage(mockBlockingQueue(10)),  2);
		demux.setSinkChannel(mockBlockingQueue(10));

		demux.pipe(mockDuplicableStageChain( 1));
		demux.unpipe();

		Assert.assertEquals(0, demux.countPipedStages());
	}

	private DuplicableStage mockDuplicableStage(BlockingQueue<Statement> sourceChannel) {

		DuplicableStage stage = Mockito.mock(DuplicableStage.class);

		if (sourceChannel != null) {
			Mockito.when(stage.getSourceChannel()).thenReturn(sourceChannel);
		}
		return stage;
	}

	private DuplicableStageChain mockDuplicableStageChain(int duplication) {

		DuplicableStage stage = Mockito.mock(DuplicableStage.class);

		DuplicableStageChain chain = Mockito.mock(DuplicableStageChain.class);
		Mockito.when(chain.getHead()).thenReturn(stage);

		List<DuplicableStageChain> chains = new ArrayList<>();

		for (int i=0 ; i<duplication ; i++) {
			chains.add(chain);
		}

		Mockito.when(chain.duplicateNTimes(10, duplication)).thenReturn(chains);

		return chain;
	}

	private BlockingQueue<Statement> mockBlockingQueue(int capacity) {

		BlockingQueue<Statement> bq = Mockito.mock(BlockingQueue.class);

		Mockito.when(bq.remainingCapacity()).thenReturn(capacity);
		return bq;
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