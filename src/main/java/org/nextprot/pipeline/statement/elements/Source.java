package org.nextprot.pipeline.statement.elements;

import org.nextprot.commons.statements.Statement;
import org.nextprot.commons.statements.reader.BufferableStatementReader;
import org.nextprot.commons.statements.reader.BufferedJsonStatementReader;
import org.nextprot.pipeline.statement.PipelineElement;
import org.nextprot.pipeline.statement.Pump;
import org.nextprot.pipeline.statement.elements.runnable.AbstractRunnablePipelineElement;
import org.nextprot.pipeline.statement.ports.SinkPipePort;
import org.nextprot.pipeline.statement.ports.SourcePipePort;

import java.io.IOException;
import java.io.Reader;
import java.util.List;

/**
 * This class is a source of data for a pipe of threads.
 * It pumps statements and send them in a connected receiver
 * but cannot serve as a receiver for any other Pipe: it must always be at the beginning,
 * or "source" of the pipe.
 **/
public class Source extends BasePipelineElement<PipelineElement> {

	private Pump<Statement> pump;

	public Source(Pump<Statement> pump) {

		super(null, new SourcePipePort(pump.capacity()));
		this.pump = pump;
	}

	private int pump(List<Statement> buffer) throws IOException {

		return pump.pump(buffer);
	}

	@Override
	public void unpipe() throws IOException {

		pump.stop();
		super.unpipe();
	}

	@Override
	public SinkPipePort getSinkPipePort() {

		throw new Error("It is a Source, can't connect to a PipelineElement through this pipe!");
	}

	@Override
	public RunnableSource newRunnableElement() {

		return new RunnableSource(this);
	}

	public static class StatementPump implements Pump<Statement> {

		private final BufferableStatementReader reader;
		private final int capacity;

		public StatementPump(Reader reader) throws IOException {

			this(reader, 100);
		}

		public StatementPump(Reader reader, int capacity) throws IOException {

			this.reader = new BufferedJsonStatementReader(reader, capacity);
			this.capacity = capacity;
		}

		@Override
		public Statement pump() throws IOException {

			return reader.nextStatement();
		}

		@Override
		public int capacity() {

			return capacity;
		}

		@Override
		public int pump(List<Statement> collector) throws IOException {

			return reader.readStatements(collector);
		}

		@Override
		public boolean isEmpty() throws IOException {

			return reader.hasStatement();
		}

		@Override
		public void stop() throws IOException {

			reader.close();
		}
	}

	private static class RunnableSource extends AbstractRunnablePipelineElement<Source> {

		private RunnableSource(Source source) {

			super(source.pump.capacity(), source);
		}

		@Override
		public boolean handleFlow(List<Statement> buffer) throws IOException {

			int stmtsRead;

			Source source = pipelineElement;

			while ((stmtsRead = source.pump(buffer)) != -1) {

				source.getSourcePipePort().write(buffer, 0, stmtsRead);
				buffer.clear();
			}

			source.getSourcePipePort().write(END_OF_FLOW_TOKEN);
			return true;
		}
	}
}
