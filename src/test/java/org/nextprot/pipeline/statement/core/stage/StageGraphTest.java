package org.nextprot.pipeline.statement.core.stage;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.nextprot.pipeline.statement.core.stage.source.PumpingSource;

import static org.junit.Assert.*;

public class StageGraphTest {

	@Test
	public void shouldConstrEmptyGraph() {

		StageGraph graph = new StageGraph();
		Assert.assertEquals(0, graph.countNodes());
		Assert.assertEquals(0, graph.countEdges());
	}

	@Test
	public void addNode() {

		StageGraph graph = new StageGraph();

		Source stage = mockSource();

		graph.addNode(stage);
		Assert.assertEquals(1, graph.countNodes());
		Assert.assertEquals(0, graph.countEdges());
		Assert.assertEquals(0, graph.getStageId(stage));
	}

	@Test
	public void getStageId() {
	}

	@Test
	public void addEdge() {
	}

	private Source mockSource() {

		Source source = Mockito.mock(Source.class);

		Mockito.when(source.getName()).thenReturn("Source");

		return source;
	}
}