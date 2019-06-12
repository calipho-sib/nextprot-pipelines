package org.nextprot.pipeline.statement.elements;

import org.nextprot.commons.statements.Statement;
import org.nextprot.commons.statements.reader.BufferableStatementReader;
import org.nextprot.commons.statements.reader.BufferedJsonStatementReader;
import org.nextprot.pipeline.statement.PipelineElement;
import org.nextprot.pipeline.statement.Pump;
import org.nextprot.pipeline.statement.ports.SinkPipePort;
import org.nextprot.pipeline.statement.ports.SourcePipePort;

import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.function.BiFunction;

/**
 * This class is a source of data for a pipe of threads.
 * It pumps statements and send them in a connected receiver
 * but cannot serve as a receiver for any other Pipe: it must always be at the beginning,
 * or "source" of the pipe.
 **/
public class Source extends BasePipelineElement<PipelineElement> {

	private BiFunction<Reader, Integer, Pump<Statement>> pumpSupplier;
	private Reader reader;
	private ThreadLocal<Pump<Statement>> pump;

	public Source(Reader reader, int pumpCapacity) {

		this(reader, pumpCapacity, (r, pc) -> {
			try {
				return new StatementPump(r, pc);
			} catch (IOException e) {
				e.printStackTrace();
				throw new IllegalArgumentException(e.getMessage());
			}
		});

		this.pump = new ThreadLocal<>();
	}

	public Source(Reader reader, int pumpCapacity, BiFunction<Reader, Integer, Pump<Statement>> pumpSupplier) {

		super(pumpCapacity, null, new SourcePipePort(pumpCapacity));
		this.reader = reader;
		this.pumpSupplier = pumpSupplier;
	}

	@Override
	public boolean handleFlow(List<Statement> buffer) throws IOException {

		int stmtsRead;

		while ((stmtsRead = pump.get().pump(buffer)) != -1) {

			getSourcePipePort().write(buffer, 0, stmtsRead);
			buffer.clear();
		}

		getSourcePipePort().write(END_OF_FLOW_TOKEN);
		return true;
	}

	@Override
	public void endOfFlow() { }

	@Override
	public String getThreadName() {
		return "Source";
	}

	@Override
	public void stop() throws IOException {

		pump.get().close();
		super.stop();
	}

	@Override
	public void elementOpened(int capacity) {

		pump.set(pumpSupplier.apply(reader, capacity));
	}

	@Override
	public SinkPipePort getSinkPipePort() {

		throw new Error("It is a Source, can't connect to a PipelineElement through this pipe!");
	}

	@Override
	public void statementsHandled(int statements) { }

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
		public void close() throws IOException {

			reader.close();
		}
	}

	/*public static class Logged extends LoggedElement {

		public Logged(Source source, String path) {

			this(source, () -> {
				try {
					return new PrintStream(new File(path+File.separator+source.getThreadName()+".log"));
				} catch (FileNotFoundException e) {
					throw new IllegalArgumentException("Cannot create Logged Source", e);
				}
			});
		}

		public Logged(BasePipelineElement pipelineElement, Supplier<PrintStream> eventHandlerFunction) {

			super(pipelineElement, eventHandlerFunction);
		}

		@Override
		public void elementOpened(int capacity) { }

		@Override
		public void statementsHandled(int stmtsRead) {
			sendMessage("pump "+ stmtsRead + " statements");
		}

		@Override
		public void elementClosed() {
			sendMessage("pump stopped");
		}
	}*/
}
