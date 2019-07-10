package org.nextprot.pipeline.statement.nxflat;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.core.Pipeline;
import org.nextprot.pipeline.statement.core.PipelineBuilder;
import org.nextprot.pipeline.statement.core.elements.Sink;
import org.nextprot.pipeline.statement.core.elements.flowable.BaseFlowLog;
import org.nextprot.pipeline.statement.core.elements.flowable.BaseValve;
import org.nextprot.pipeline.statement.core.elements.flowable.FlowEventHandler;
import org.nextprot.pipeline.statement.core.elements.source.Pump;
import org.nextprot.pipeline.statement.nxflat.source.pump.WebStatementPump;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Thread.sleep;
import static org.nextprot.pipeline.statement.core.elements.Source.POISONED_STATEMENT;


public class LoadVariantFrequencies {

	public static void main(String[] args) throws IOException {

		Timer timer = new Timer();

		// Note: npteam@kant:/share/sib/calipho/nxflat-proxy/statements-downloader/gnomad/2019-03-14/
		URL url = new URL("http://kant.sib.swiss:9001/gnomad/2019-03-14/chr22.json");

		Pump<Statement> pump = new WebStatementPump(url);

		try {
			Pipeline pipeline = new PipelineBuilder()
					.start(timer)
					.source(pump)
					.split(() -> new VariantFrequencySink(1000), 10)
					.build();

			pipeline.openValves();

			// Wait for the pipe to complete
			try {
				pipeline.waitForThePipesToComplete();
			} catch (InterruptedException e) {
				System.err.println("pipeline error: " + e.getMessage());
			}

			System.out.println("Done in "+timer.getElapsedTimeInMs() + " ms.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static class VariantFrequencySink extends Sink {

		private final int bufferSize;

		private VariantFrequencySink(int bufferSize) {

			this.bufferSize = bufferSize;
		}

		@Override
		public Valve newValve() {

			return new Valve(this);
		}

		@Override
		public VariantFrequencySink duplicate(int newCapacity) {

			return new VariantFrequencySink(this.bufferSize);
		}
	}

	private static class Valve extends BaseValve<VariantFrequencySink> {

		private final List<Statement> buffer;
		private final int bufferSize;

		private Valve(VariantFrequencySink sink) {
			super(sink);

			buffer = new ArrayList<>(sink.bufferSize);
			this.bufferSize = sink.bufferSize;
		}

		@Override
		public boolean handleFlow(VariantFrequencySink sink) throws Exception {

			Statement statement = sink.getSinkChannel().take();

			buffer.add(statement);
			getFlowEventHandler().statementHandled(statement);

			boolean poisoned = statement == POISONED_STATEMENT;

			if (poisoned || buffer.size() % bufferSize == 0) {

				loadStatements();
				buffer.clear();

				sleep(100);
			}

			return poisoned;
		}

		private void loadStatements() {

			// ... do the API call to the service doing the load (POST json )
			// allele_count
			// allele_number
			// rs_id

			((FlowLog)getFlowEventHandler()).statementsLoaded(buffer);
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

			sendMessage("load " + statements.size()+" statements");
		}

		@Override
		public void endOfFlow() {

			sendMessage(getStatementCount() + " statements loaded in table variant_frequencies");
		}
	}
}
