package org.nextprot.pipeline.statement;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.pipes.Sink;

import java.io.IOException;
import java.util.function.Function;

public class PipelineBuilder implements Pipeline.StartStep {

	private final Pipeline.DataCollector dataCollector = new Pipeline.DataCollector();

	@Override
	public Pipeline.SourceStep start(Pipeline.Monitorable monitorable) {

		dataCollector.setMonitorable(monitorable);
		return new Source();
	}

	public class Source implements Pipeline.SourceStep {

		@Override
		public Pipeline.FilterStep source(Pump<Statement> pump) {

			final org.nextprot.pipeline.statement.pipes.Source source = new org.nextprot.pipeline.statement.pipes.Source(pump);
			dataCollector.setSource(source);

			return new FilterStep(source);
		}
	}

	public class FilterStep implements Pipeline.FilterStep {

		private final Pipe source;

		FilterStep(Pipe source) {

			this.source = source;
		}

		@Override
		public Pipeline.FilterStep filter(Function<Integer, Filter> filterProvider) throws IOException {

			Filter pipedFilter = filterProvider.apply(dataCollector.getSource().getPump().capacity());
			source.connect(pipedFilter);

			return new FilterStep(pipedFilter);
		}

		@Override
		public Pipeline.TerminateStep sink(Function<Integer, Sink> sinkProvider) throws IOException {

			Sink sink = sinkProvider.apply(1);
			source.connect(sink);

			return new TerminateStep();
		}

		public class TerminateStep implements Pipeline.TerminateStep {

			@Override
			public Pipeline build() {

				return new Pipeline(dataCollector);
			}
		}
	}

}
