package org.nextprot.pipeline.statement;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.elements.Sink;
import org.nextprot.pipeline.statement.elements.source.Pump;
import org.nextprot.pipeline.statement.elements.source.PumpBasedSource;
import org.nextprot.pipeline.statement.elements.demux.Demultiplexer;
import org.nextprot.pipeline.statement.elements.demux.DuplicableElement;

import java.util.function.Function;
import java.util.function.Supplier;

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

			final PumpBasedSource source = new PumpBasedSource(pump);
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
		public Pipeline.FilterStep filter(Function<Integer, DuplicableElement> filterProvider) {

			DuplicableElement pipedFilter = filterProvider.apply(previousElement.getSourceChannel().remainingCapacity());
			previousElement.pipe(pipedFilter);

			return new FilterStep(pipedFilter);
		}

		@Override
		public Pipeline.FilterStep demuxFilter(Function<Integer, DuplicableElement> filterProvider, int sourcePipePortCount) {

			DuplicableElement pipedFilter = filterProvider.apply(previousElement.getSourceChannel().remainingCapacity());
			previousElement.pipe(pipedFilter);

			dataCollector.setDemuxSourcePipePortCount(sourcePipePortCount);
			dataCollector.setDemuxFromElement(previousElement, pipedFilter);

			return new FilterStep(pipedFilter);
		}

		@Override
		public Pipeline.TerminateStep sink(Supplier<Sink> sinkProvider) {

			Sink sink = sinkProvider.get();
			previousElement.pipe(sink);

			return new TerminateStep();
		}

		public class TerminateStep implements Pipeline.TerminateStep {

			@Override
			public Pipeline build() {

				if (dataCollector.getDemuxFromElement() != null) {

					DuplicableElement fromElement = dataCollector.getDemuxFromElement();

					Demultiplexer demultiplexer = new Demultiplexer(fromElement.getSinkChannel().remainingCapacity(),
							dataCollector.getDemuxSourcePipePortCount());

					demultiplexer.pipe(fromElement);
					dataCollector.getElementBeforeDemux().pipe(demultiplexer);
				}

				return new Pipeline(dataCollector);
			}
		}
	}

}
