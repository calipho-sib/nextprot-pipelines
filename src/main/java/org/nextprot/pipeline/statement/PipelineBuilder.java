package org.nextprot.pipeline.statement;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.elements.Sink;
import org.nextprot.pipeline.statement.elements.Source;
import org.nextprot.pipeline.statement.muxdemux.Demultiplexer;
import org.nextprot.pipeline.statement.muxdemux.DuplicableElement;

import java.io.IOException;
import java.util.function.Function;

public class PipelineBuilder implements Pipeline.StartStep {

	private final Pipeline.DataCollector dataCollector = new Pipeline.DataCollector();

	@Override
	public Pipeline.SourceStep start(Pipeline.Monitorable monitorable) {

		dataCollector.setMonitorable(monitorable);
		return new SourceStep();
	}

	public class SourceStep implements Pipeline.SourceStep {

		@Override
		public Pipeline.FilterStep source(Pump<Statement> pump) {

			final Source source = new Source(pump);
			dataCollector.setSource(source);

			return new FilterStep(source);
		}
	}

	public class FilterStep implements Pipeline.FilterStep {

		private final PipelineElement source;

		FilterStep(PipelineElement source) {

			this.source = source;
		}

		@Override
		public Pipeline.FilterStep filter(Function<Integer, Filter> filterProvider) throws IOException {

			Filter pipedFilter = filterProvider.apply(dataCollector.getSource().getPump().capacity());
			source.pipe(pipedFilter);

			return new FilterStep(pipedFilter);
		}

		@Override
		public Pipeline.FilterStep demux(DuplicableElement element, int sourcePipePortCount) throws IOException {

			Demultiplexer demux = new Demultiplexer(element.getSinkPipePort(), sourcePipePortCount);
			demux.pipe(element);

			return null;
		}

		@Override
		public Pipeline.TerminateStep sink(Function<Integer, Sink> sinkProvider) throws IOException {

			Sink sink = sinkProvider.apply(1);
			source.pipe(sink);

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
