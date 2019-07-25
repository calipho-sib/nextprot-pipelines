package org.nextprot.pipeline.statement.core.stage;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.nextprot.pipeline.statement.core.Stage;

import java.util.List;

public class StageGraphTest {

	@Test(expected = NullPointerException.class)
	public void shouldNotConstrGraphFromNullSource() {

		new StageGraph(null);
	}

	@Test
	public void shouldConstr1NodeGraph() {

		StageGraph graph = new StageGraph(mockSource());
		Assert.assertEquals(1, graph.countNodes());
		Assert.assertEquals(0, graph.countEdges());
		Assert.assertNotNull(graph.getSource());
	}

	@Test
	public void addNode() {

		StageGraph graph = new StageGraph(mockSource());

		Stage stage = mockStage();
		graph.addNode(stage);

		Assert.assertEquals(2, graph.countNodes());
		Assert.assertEquals(0, graph.countEdges());
		Assert.assertEquals(1, graph.getStageId(stage));
	}

	@Test
	public void addSameNodes() {

		StageGraph graph = new StageGraph(mockSource());

		Stage stage = mockStage();

		graph.addNode(stage);
		graph.addNode(stage);

		Assert.assertEquals(2, graph.countNodes());
		Assert.assertEquals(0, graph.countEdges());
	}

	@Test
	public void addEdgeFromSource() {

		StageGraph graph = new StageGraph(mockSource());

		Stage stage = mockStage();

		graph.addNode(stage);
		graph.addEdgeFromSource(stage);

		Assert.assertEquals(1, graph.countEdges());
	}

	@Test
	public void addEdge() {

		StageGraph graph = new StageGraph(mockSource());

		Stage stage1 = mockStage("stage 1");
		Stage stage2 = mockStage("stage 2");

		graph.addEdge(stage1, stage2);

		Assert.assertEquals(1, graph.countEdges());
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldNotAddEdgeBeforeSource() {

		StageGraph graph = new StageGraph(mockSource());

		Source source = mockSource();
		Stage stage = mockStage();

		graph.addEdge(stage, source);
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldNotAddEdgeAfterSink() {

		StageGraph graph = new StageGraph(mockSource());

		Stage stage = mockStage();
		Sink sink = mockSink();

		graph.addEdge(sink, stage);
	}

	@Test
	public void createGraph() {

		StageGraph graph = new StageGraph(mockSource());

		Stage stage1 = mockStage("stage 1");
		Stage stage2 = mockStage("stage 2");
		Sink sink = mockSink();

		graph.addEdgeFromSource(stage1);
		graph.addEdge(stage1, stage2);
		graph.addEdge(stage2, sink);

		Assert.assertEquals(3, graph.countEdges());

		List<Stage> stages = graph.getStages();
		Assert.assertEquals(4, stages.size());
	}

	private Source mockSource() {

		Source source = Mockito.mock(Source.class);

		Mockito.when(source.getName()).thenReturn("Source");

		return source;
	}

	private Sink mockSink() {

		Sink sink = Mockito.mock(Sink.class);

		Mockito.when(sink.getName()).thenReturn("Sink");

		return sink;
	}

	private Stage mockStage() {

		return mockStage("Stage");
	}

	private Stage mockStage(String name) {

		Stage stage = Mockito.mock(Stage.class);

		Mockito.when(stage.getName()).thenReturn(name);

		return stage;
	}
}