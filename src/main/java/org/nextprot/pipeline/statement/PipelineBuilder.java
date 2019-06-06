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

		private final PipelineElement previousElement;


		FilterStep(PipelineElement previousElement) {

			this.previousElement = previousElement;
		}

		@Override
		public Pipeline.FilterStep filter(Function<Integer, DuplicableElement> filterProvider) throws IOException {

			DuplicableElement pipedFilter = filterProvider.apply(previousElement.getSourcePipePort().capacity());
			previousElement.pipe(pipedFilter);

			return new FilterStep(pipedFilter);
		}

		@Override
		public Pipeline.FilterStep filter(Function<Integer, DuplicableElement> filterProvider, int sourcePipePortCount) throws IOException {

			DuplicableElement pipedFilter = filterProvider.apply(previousElement.getSourcePipePort().capacity());
			previousElement.pipe(pipedFilter);

			dataCollector.setDemuxSourcePipePortCount(sourcePipePortCount);
			dataCollector.setDemuxFromElement(pipedFilter);

			return new FilterStep(pipedFilter);
		}

		@Override
		public Pipeline.TerminateStep sink(Function<Integer, Sink> sinkProvider) throws IOException {

			Sink sink = sinkProvider.apply(1);
			previousElement.pipe(sink);

			return new TerminateStep();
		}

		public class TerminateStep implements Pipeline.TerminateStep {

			@Override
			public Pipeline build() throws IOException {

				if (dataCollector.demuxFromElement() != null) {

					DuplicableElement fromElement = dataCollector.demuxFromElement();

					Demultiplexer demultiplexer = new Demultiplexer(fromElement.getSinkPipePort(),
							dataCollector.getDemuxSourcePipePortCount());

					demultiplexer.pipe(fromElement);

					dataCollector.getSource().getSourcePipePort().disconnectSink();
					dataCollector.getSource().pipe(demultiplexer);
				}

				return new Pipeline(dataCollector);
			}
		}
	}

}
