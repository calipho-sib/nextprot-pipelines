package org.nextprot.pipeline.statement.elements;

import org.nextprot.commons.statements.Statement;
import org.nextprot.commons.statements.reader.BufferableStatementReader;
import org.nextprot.commons.statements.reader.BufferedJsonStatementReader;
import org.nextprot.pipeline.statement.PipelineElement;
import org.nextprot.pipeline.statement.Pump;
import org.nextprot.pipeline.statement.elements.runnable.BaseFlowLog;
import org.nextprot.pipeline.statement.elements.runnable.BaseFlowablePipelineElement;
import org.nextprot.pipeline.statement.elements.runnable.FlowEventHandler;
import sun.net.www.protocol.http.HttpURLConnection;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * This class is a source of data for a pipe of threads.
 * It pumps statements and send them in a connected receiver
 * but cannot serve as a receiver for any other Pipe: it must always be at the beginning,
 * or "source" of the pipe.
 **/
public class Source extends BasePipelineElement<PipelineElement> {

	private Pump<Statement> pump;

	public Source(Pump<Statement> pump) {

		super(pump.capacity());
		this.pump = pump;
	}

	private Statement pump() throws IOException {

		return pump.pump();
	}

	private int pump(List<Statement> buffer) throws IOException {

		return pump.pump(buffer);
	}

	@Override
	public void closeValves() throws IOException {

		pump.stop();
		super.closeValves();
	}

	@Override
	public BlockingQueue<Statement> getSinkChannel() {

		throw new Error("It is a Source element, can't connect to a PipelineElement through this channel!");
	}

	@Override
	public Flowable newFlowable() {

		return new Flowable(this, pump.capacity());
	}

	public static class WebStatementPump implements Pump<Statement> {

		private final BufferableStatementReader reader;
		private final int capacity;

		public WebStatementPump(URL url) throws IOException {

			this(url, 100);
		}

		public WebStatementPump(URL url, int capacity) throws IOException {

			if (!isServiceUp(url)) {
				throw new IOException("Cannot create a pump: " + url + " is not reachable");
			}

			this.capacity = capacity;
			this.reader = new BufferedJsonStatementReader(new InputStreamReader(url.openStream()), capacity);
		}

		private static boolean isServiceUp(URL url) throws IOException {

			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("HEAD");
			connection.setConnectTimeout(3000);
			connection.setReadTimeout(3000);

			try {
				connection.connect();

				return connection.getResponseCode() == HttpURLConnection.HTTP_OK;
			} catch (IOException e) {

				throw new IOException("statement service " + url + " does not respond: " + e.getMessage());
			} finally {

				connection.disconnect();
			}
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

	private static class Flowable extends BaseFlowablePipelineElement<Source> {

		private final int capacity;

		private Flowable(Source source, int capacity) {

			super(source);
			this.capacity = capacity;
		}

		@Override
		public boolean handleFlow(Source source) throws Exception {

			FlowEventHandler eh = flowEventHandlerHolder.get();

			Statement statement = source.pump();

			if (statement == null) {
				statement = END_OF_FLOW_STATEMENT;
			}

			source.getSourceChannel().put(statement);
			eh.statementHandled(statement);

			return statement == END_OF_FLOW_STATEMENT;
		}

		@Override
		public FlowEventHandler createEventHandler() throws FileNotFoundException {

			return new FlowLog(getThreadName(), capacity);
		}
	}

	private static class FlowLog extends BaseFlowLog {

		private final int capacity;

		public FlowLog(String threadName, int capacity) throws FileNotFoundException {

			super(threadName);
			this.capacity = capacity;
		}

		@Override
		public void beginOfFlow() {

			sendMessage("pump started (capacity="+ capacity + ")");
		}

		@Override
		public void statementHandled(Statement statement) {

			super.statementHandled(statement);
			sendMessage("pump statement " + getStatementId(statement));
		}

		@Override
		public void endOfFlow() {

			sendMessage(getStatementCount()+" statements pumped");
		}
	}
}
