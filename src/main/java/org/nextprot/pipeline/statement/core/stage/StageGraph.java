package org.nextprot.pipeline.statement.core.stage;

import com.google.common.base.Preconditions;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.custom_hash.TObjectIntCustomHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.strategy.HashingStrategy;
import org.nextprot.commons.graph.DirectedGraph;
import org.nextprot.commons.graph.IntGraph;
import org.nextprot.pipeline.statement.core.Pipeline;
import org.nextprot.pipeline.statement.core.Stage;

public class StageGraph {

	private static int COUNTER = 0;

	private final DirectedGraph graph;
	private final TIntObjectMap<Stage> stagesByNodeId;
	private final TObjectIntMap<Stage> stages;

	public StageGraph() {

		this.graph = new IntGraph();
		this.stagesByNodeId = new TIntObjectHashMap<>();
		this.stages = new TObjectIntCustomHashMap<>(new Strategy());
	}

	public int countNodes() {

		return graph.countNodes();
	}

	public int countEdges() {

		return graph.countEdges();
	}

	public void addNode(Stage stage) {

		Preconditions.checkNotNull(stage);

		int id = COUNTER;

		this.graph.addNode(id);
		stagesByNodeId.put(id, stage);
		stages.put(stage, id);

		COUNTER++;
	}

	public int getStageId(Stage stage) {

		if (!stages.containsKey(stage)) {
			throw new IllegalArgumentException("stage "+ stage.getName() + " was not found in the graph");
		}

		return stages.get(stage);
	}

	public void addEdge(Stage from, Stage to) {

		if (from instanceof Sink) {
			throw new IllegalArgumentException(from.getName() + " is a sink and cannot be piped to stage "+ to.getName());
		}

		if (to instanceof Source) {
			throw new IllegalArgumentException(to.getName() + " is a source and cannot be piped to stage "+ from.getName());
		}

		if (!stages.containsKey(from)) {
			addNode(from);
		}

		if (!stages.containsKey(to)) {
			addNode(to);
		}

		this.graph.addEdge(getStageId(from), getStageId(to));
	}

	private static class Strategy implements HashingStrategy<Stage> {

		@Override
		public int computeHashCode(Stage stage) {

			return stage.getName().hashCode();
		}

		@Override
		public boolean equals(Stage stage1, Stage stage2) {

			return stage1.getName().equals(stage2.getName());
		}
	}
}
