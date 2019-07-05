package org.nextprot.pipeline.statement.nxflat;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.core.Pipeline;
import org.nextprot.pipeline.statement.core.PipelineBuilder;
import org.nextprot.pipeline.statement.core.elements.Sink;
import org.nextprot.pipeline.statement.core.elements.flowable.BaseFlowLog;
import org.nextprot.pipeline.statement.core.elements.flowable.BaseFlowablePipelineElement;
import org.nextprot.pipeline.statement.core.elements.flowable.FlowEventHandler;
import org.nextprot.pipeline.statement.core.elements.source.Pump;
import org.nextprot.pipeline.statement.nxflat.source.pump.WebStatementPump;

import java.io.FileNotFoundException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static org.nextprot.pipeline.statement.core.elements.Source.POISONED_STATEMENT;


public class LoadVariantFrequencies {

	public static void main(String[] args) throws Exception {

		Timer timer = new Timer();

		// Note: npteam@kant:/share/sib/calipho/nxflat-proxy/statements-downloader/gnomad/2019-03-14/
		URL url = new URL("http://kant.sib.swiss:9001/gnomad/2019-03-14/chr22.json");

		Pump<Statement> pump = new WebStatementPump(url);

		Pipeline pipeline = new PipelineBuilder()
				.start(timer)
				.source(pump)
				.split(() -> new VariantFrequencySink(1000), 10)
				.build();

		pipeline.openValves();
		pipeline.waitForThePipesToComplete();

		System.out.println("Done in " + timer.getElapsedTimeInMs() + " ms.");
	}

	private static class VariantFrequencySink extends Sink {

		private final int bufferSize;

		private VariantFrequencySink(int bufferSize) {

			this.bufferSize = bufferSize;
		}

		@Override
		public Flowable newFlowable() {

			return new Flowable(this);
		}

		@Override
		public VariantFrequencySink duplicate(int newCapacity) {

			return new VariantFrequencySink(this.bufferSize);
		}
	}

	private static class Flowable extends BaseFlowablePipelineElement<VariantFrequencySink> {

		private final List<Statement> buffer;
		private final int bufferSize;

		private Flowable(VariantFrequencySink sink) {
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

				wait(100);
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

			return new FlowLog(getThreadName());
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
