package org.nextprot.pipeline.statement.elements;

import org.nextprot.commons.statements.Statement;
import org.nextprot.commons.statements.reader.BufferableStatementReader;
import org.nextprot.commons.statements.reader.BufferedJsonStatementReader;
import org.nextprot.pipeline.statement.Demux;
import org.nextprot.pipeline.statement.PipelineElement;
import org.nextprot.pipeline.statement.Pump;
import org.nextprot.pipeline.statement.elements.runnable.BaseFlowLog;
import org.nextprot.pipeline.statement.elements.runnable.BaseFlowablePipelineElement;
import org.nextprot.pipeline.statement.elements.runnable.FlowEventHandler;
import org.nextprot.pipeline.statement.muxdemux.Demultiplexer;
import sun.net.www.protocol.http.HttpURLConnection;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public abstract class Source extends BasePipelineElement<PipelineElement> {

	protected Source(int sourceCapacity) {

		super(sourceCapacity);
	}

	protected Source(BlockingQueue<Statement> sourceChannel) {

		super(sourceChannel);
	}

	@Override
	public BlockingQueue<Statement> getSinkChannel() {

		throw new Error("It is a Source element, can't connect to a PipelineElement through this channel!");
	}

	protected int countPoisonedPillsToProduce() {

		PipelineElement element = this;

		while ((element = element.nextElement()) != null) {

			if (element instanceof Demux) {
				return ((Demux)element).countSourceChannels();
			}
		}
		return 1;
	}
}
