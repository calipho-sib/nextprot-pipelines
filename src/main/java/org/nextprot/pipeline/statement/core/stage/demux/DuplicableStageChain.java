package org.nextprot.pipeline.statement.core.stage.demux;

import org.nextprot.pipeline.statement.core.stage.DuplicableStage;
import org.nextprot.pipeline.statement.core.stage.Sink;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A chain of duplicable stages from head to sink
 */
public class DuplicableStageChain {

	private final List<DuplicableStage> headToSinkStages;

	public DuplicableStageChain(DuplicableStage headStage) {

		this(getStagesUntilSink(headStage));
	}

	public DuplicableStageChain(List<DuplicableStage> headToSinkStages) {

		this.headToSinkStages = new ArrayList<>(headToSinkStages);
	}

	public Stream<DuplicableStage> stream() {
		return headToSinkStages.stream();
	}

	public boolean isEmpty() {
		return headToSinkStages.isEmpty();
	}

	public int size() {
		return headToSinkStages.size();
	}

	public DuplicableStage getHead() {

		return headToSinkStages.get(0);
	}

	public DuplicableStage getLast() {

		return headToSinkStages.get(headToSinkStages.size()-1);
	}

	/**
	 * Duplicate stages from HEAD to SINK and pipe them
	 *
	 * @param capacity the channels capacity
	 * @return the head stage of the chain
	 */
	public DuplicableStageChain duplicate(int capacity) {

		if (! (getLast() instanceof Sink) ) {

			throw new IllegalStateException("cannot duplicate from HEAD stage "+
					getHead().getName() + ": the last stage is not a SINK");
		}

		List<DuplicableStage> duplicatedStages = headToSinkStages.stream()
				.map(stage -> stage.duplicate(capacity))
				.collect(Collectors.toList());

		pipeStages(duplicatedStages);

		return new DuplicableStageChain(duplicatedStages.get(0));
	}

	/**
	 * Pipe stages together
	 * @param stages the stages to pipe together
	 * @return the head of the new piped stages
	 */
	private void pipeStages(List<DuplicableStage> stages) {

		for (int i = 1; i < stages.size(); i++) {

			stages.get(i - 1).pipe(stages.get(i));
		}
	}

	private static List<DuplicableStage> getStagesUntilSink(DuplicableStage headStage) {

		List<DuplicableStage> stageList = new ArrayList<>();

		stageList.add(headStage);

		DuplicableStage stage = headStage;

		while ((stage = stage.getFirstPipedStage()) != null) {

			stageList.add(stage);
		}

		return stageList;
	}
}
