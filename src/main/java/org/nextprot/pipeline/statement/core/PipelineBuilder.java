package org.nextprot.pipeline.statement.core;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.core.stage.Sink;
import org.nextprot.pipeline.statement.core.stage.filter.BaseFilter;
import org.nextprot.pipeline.statement.core.stage.source.Pump;
import org.nextprot.pipeline.statement.core.stage.source.PumpingSource;
import org.nextprot.pipeline.statement.core.stage.Demultiplexer;
import org.nextprot.pipeline.statement.core.stage.DuplicableStage;

import java.io.FileNotFoundException;
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
		public Pipeline.FilterStep source(Pump<Statement> pump, int capacity) {

			final PumpingSource source = new PumpingSource(pump, capacity);
			dataCollector.setSource(source);

			return new FilterStep(source);
		}
	}

	public class FilterStep implements Pipeline.FilterStep {

		private final Stage previousElement;


		FilterStep(Stage previousElement) {

			this.previousElement = previousElement;
		}

		@Override
		public Pipeline.FilterStep filter(Function<Integer, DuplicableStage> filterProvider) {

			DuplicableStage pipedFilter = filterProvider.apply(previousElement.getSourceChannel().remainingCapacity());
			previousElement.pipe(pipedFilter);

			return new FilterStep(pipedFilter);
		}

		@Override
		public Pipeline.FilterStep split(Function<Integer, BaseFilter> filterProvider, int partitionCount) {

			BaseFilter pipedFilter = filterProvider.apply(previousElement.getSourceChannel().remainingCapacity());
			previousElement.pipe(pipedFilter);

			dataCollector.setDemuxSourceCount(partitionCount);
			dataCollector.setDemuxFromElement(previousElement, pipedFilter);

			return new FilterStep(pipedFilter);
		}

		@Override
		public Pipeline.TerminateStep split(Supplier<Sink> sinkProvider, int partitionCount) {

			Sink pipedSink = sinkProvider.get();
			previousElement.pipe(pipedSink);

			dataCollector.setDemuxSourceCount(partitionCount);
			dataCollector.setDemuxFromElement(previousElement, pipedSink);

			return new TerminateStep();
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

					DuplicableStage fromElement = dataCollector.getDemuxFromElement();

					Demultiplexer demultiplexer = new Demultiplexer(fromElement.getSinkChannel().remainingCapacity(),
							dataCollector.getDemuxSourceCount());

					demultiplexer.pipe(fromElement);
					dataCollector.getElementBeforeDemux().pipe(demultiplexer);
				}

				try {
					return new Pipeline(dataCollector);
				} catch (FileNotFoundException e) {

					throw new IllegalStateException("cannot create log files: "+e.getMessage());
				}
			}
		}
	}

}
