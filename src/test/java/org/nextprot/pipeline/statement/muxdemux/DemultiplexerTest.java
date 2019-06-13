package org.nextprot.pipeline.statement.muxdemux;

public class DemultiplexerTest {

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