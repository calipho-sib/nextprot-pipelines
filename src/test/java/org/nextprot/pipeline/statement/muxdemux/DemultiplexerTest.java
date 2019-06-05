package org.nextprot.pipeline.statement.muxdemux;

import org.junit.Test;
import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.Filter;
import org.nextprot.pipeline.statement.NarcolepticFilter;
import org.nextprot.pipeline.statement.Pump;
import org.nextprot.pipeline.statement.elements.NxFlatTableSink;
import org.nextprot.pipeline.statement.elements.Sink;
import org.nextprot.pipeline.statement.elements.Source;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class DemultiplexerTest {

	@Test
	public void connect() throws IOException, InterruptedException {

		URL url = new URL("http://kant.sib.swiss:9001/glyconnect/2019-01-22/all-entries.json");
		Reader reader = new InputStreamReader(url.openStream());
		Pump<Statement> pump = new Source.StatementPump(reader, 10);

		Source source = new Source(pump);
		Demultiplexer demux = new Demultiplexer(10, 2);
		Sink sink = new NxFlatTableSink(NxFlatTableSink.Table.entry_mapped_statements);
		Filter filter = new NarcolepticFilter(10);

		filter.pipe(sink);

		/*
		   SRC(10) --> Demux(10) --> Filter(5) --> Sink(5)
	        	             \_____  Filter(5) --> Sink(5)
		 */

		source.pipe(demux);
		demux.pipe(filter);

		List<Thread> tl = new ArrayList<>();
		demux.start(tl);

		for (Thread thread : tl) {
			thread.join();
			System.out.println("Pipe " + thread.getName() + ": closed");
		}
		System.out.println(tl);
	}
}