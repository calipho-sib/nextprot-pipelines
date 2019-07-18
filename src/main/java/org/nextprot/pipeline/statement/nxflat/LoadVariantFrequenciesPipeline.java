package org.nextprot.pipeline.statement.nxflat;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.core.Pipeline;
import org.nextprot.pipeline.statement.core.PipelineBuilder;
import org.nextprot.pipeline.statement.core.elements.Sink;
import org.nextprot.pipeline.statement.core.elements.flowable.BaseFlowLog;
import org.nextprot.pipeline.statement.core.elements.flowable.BaseValve;
import org.nextprot.pipeline.statement.core.elements.flowable.FlowEventHandler;
import org.nextprot.pipeline.statement.core.elements.source.Pump;
import org.nextprot.pipeline.statement.nxflat.source.pump.HttpStatementPump;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.Thread.sleep;
import static org.nextprot.pipeline.statement.core.elements.Source.POISONED_STATEMENT;


public class LoadVariantFrequenciesPipeline {

	public static void main(String[] args) {

		Timer timer = new Timer();

		// Note: npteam@kant:/share/sib/calipho/nxflat-proxy/statements-downloader/gnomad/2019-03-14/
		String K22URL = "http://kant.sib.swiss:9001/gnomad/2019-03-14/chr22.json";
		String variantFreqsLoadingServiceURL = "http://alpha-api.nextprot.org/statements/variants/frequencies/";

		Pump<Statement> pump = new HttpStatementPump(K22URL);

		try {
			Pipeline pipeline = new PipelineBuilder()
					.start(timer)
					.source(pump, 100)
					.split(() -> new VariantFrequencySink(variantFreqsLoadingServiceURL, 1000), 10)
					.build();

			pipeline.openValves();

			// Wait for the pipe to complete
			try {
				pipeline.waitUntilCompletion();
			} catch (InterruptedException e) {
				System.err.println("pipeline error: " + e.getMessage());
			}

			System.out.println("Done in " + timer.getElapsedTimeInMs() + " ms.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static class VariantFrequencySink extends Sink {

		private final String loadingServiceURL;
		private final int bufferSize;

		private VariantFrequencySink(String loadingServiceURL, int bufferSize) {

			this.bufferSize = bufferSize;
			this.loadingServiceURL = loadingServiceURL;
		}

		@Override
		public Valve newValve() {

			return new Valve(this);
		}

		@Override
		public VariantFrequencySink duplicate(int newCapacity) {

			return new VariantFrequencySink(this.loadingServiceURL, this.bufferSize);
		}
	}

	private static class Valve extends BaseValve<VariantFrequencySink> {

		private final List<Statement> buffer;
		private final int bufferSize;
		private final URI loadingServiceURI;

		private Valve(VariantFrequencySink sink) {
			super(sink);

			buffer = new ArrayList<>(sink.bufferSize);
			this.bufferSize = sink.bufferSize;
			try {
				loadingServiceURI = new URI(sink.loadingServiceURL);
			} catch (URISyntaxException e) {
				throw new IllegalStateException(sink.loadingServiceURL+": not a valid url");
			}
		}

		@Override
		public boolean handleFlow(VariantFrequencySink sink) throws Exception {

			Statement statement = sink.getSinkChannel().take();

			buffer.add(statement);
			getFlowEventHandler().statementHandled(statement);

			boolean poisoned = statement == POISONED_STATEMENT;

			if (poisoned || buffer.size() % bufferSize == 0) {

				loadStatements(buffer);
				buffer.clear();

				sleep(100);
			}

			return poisoned;
		}

		private void loadStatements(List<Statement> statements) throws IOException {

			// ... do the API call to the service doing the load (POST json )
			// allele_count
			// allele_number
			// rs_id
			Map<String, Object> results = postStatements(statements);

			((FlowLog) getFlowEventHandler()).statementsLoaded(statements);
		}

		public Map<String, Object> postStatements(List<Statement> statements) throws IOException {

			CloseableHttpClient client = HttpClients.createDefault();
			HttpPost post = new HttpPost(loadingServiceURI);
			post.setHeader("Accept", "application/json");

			HttpEntity entity = EntityBuilder.create()
					.setText(Statement.toJsonString(statements))
					.build();

			post.setEntity(entity);

			CloseableHttpResponse response = client.execute(post);
			Map<String, Object> rawContent = readRawJsonOutput(response);

			client.close();

			return rawContent;
		}

		private Map<String, Object> readRawJsonOutput(CloseableHttpResponse response) throws IOException {

			StringBuilder payload;

			try (BufferedReader in = new BufferedReader(
					new InputStreamReader(response.getEntity().getContent()))) {

				String line;
				payload = new StringBuilder();

				while ((line = in.readLine()) != null) {
					payload.append(line);
					payload.append(System.lineSeparator());
				}
			}

			//noinspection unchecked
			return new ObjectMapper().readValue(payload.toString(), Map.class);
		}

		@Override
		protected FlowEventHandler createFlowEventHandler() throws FileNotFoundException {

			return new FlowLog(getName());
		}
	}

	private static class FlowLog extends BaseFlowLog {

		private FlowLog(String threadName) throws FileNotFoundException {

			super(threadName);
		}

		@Override
		public void beginOfFlow() {

			sendMessage("opened");
		}

		@Override
		public void statementHandled(Statement statement) {

			super.statementHandled(statement);

			if (statement != POISONED_STATEMENT) {
				sendMessage("add statement " + statement.getStatementId());
			}
		}


		void statementsLoaded(List<Statement> statements) {

			sendMessage("load " + statements.size() + " statements");
		}

		@Override
		public void endOfFlow() {

			sendMessage(getStatementCount() + " statements loaded in table variant_frequencies");
		}
	}
}
