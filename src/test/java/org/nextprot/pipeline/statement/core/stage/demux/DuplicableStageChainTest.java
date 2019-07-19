package org.nextprot.pipeline.statement.core.stage.demux;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.MockitoInvocationHandler;
import org.nextprot.pipeline.statement.core.stage.DuplicableStage;
import org.nextprot.pipeline.statement.core.stage.Sink;
import org.nextprot.pipeline.statement.core.stage.filter.BaseFilter;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.Mockito.times;

public class DuplicableStageChainTest {

	@Test
	public void singleStageChainShouldNotBeEmpty() {

		Sink sink = mockSink();

		DuplicableStageChain chain = new DuplicableStageChain(sink);

		Assert.assertFalse(chain.isEmpty());
		Assert.assertEquals(1, chain.size());
		Assert.assertEquals(sink, chain.getHead());
		Assert.assertEquals(sink, chain.getLast());
	}

	@Test
	public void chainShouldNotBeEmpty() {

		Sink sink = mockSink();
		BaseFilter filter = mockFilter(sink);

		DuplicableStageChain chain = new DuplicableStageChain(filter);

		Assert.assertFalse(chain.isEmpty());
		Assert.assertEquals(2, chain.size());
		Assert.assertEquals(filter, chain.getHead());
		Assert.assertEquals(sink, chain.getLast());
	}

	@Test
	public void chainShouldStreamStages() {

		Sink sink = mockSink();
		BaseFilter filter = mockFilter(sink);

		DuplicableStageChain chain = new DuplicableStageChain(filter);

		Assert.assertNotNull(chain.stream());
	}

	@Test
	public void duplicateShouldCallSinkDuplicate() {

		Sink sink = mockSink();
		BaseFilter filter = mockFilter(sink);

		DuplicableStageChain duplicator = new DuplicableStageChain(filter);
		duplicator.duplicate(2);

		Mockito.verify(filter.duplicate(Mockito.anyInt()), times(2));
		Mockito.verify(sink.duplicate(Mockito.anyInt()), times(2));
	}

	@Test
	public void duplicateNTimesShouldCallSinkDuplicate() {

		Sink sink = mockSink();
		BaseFilter filter = mockFilter(sink);

		DuplicableStageChain duplicator = new DuplicableStageChain(filter);
		List<DuplicableStageChain> chains = duplicator.duplicateNTimes(2, 2);

		Mockito.verify(filter.duplicate(Mockito.anyInt()), times(4));
		Mockito.verify(sink.duplicate(Mockito.anyInt()), times(4));
		Assert.assertEquals(2, chains.size());
	}

	@Test(expected = IllegalStateException.class)
	public void cannotDuplicateIfLastNotSink() {

		BaseFilter filter = Mockito.mock(BaseFilter.class);

		DuplicableStageChain duplicator = new DuplicableStageChain(filter);
		duplicator.duplicate(2);
	}

	private BaseFilter mockFilter(Sink sink) {

		BaseFilter filter = Mockito.mock(BaseFilter.class);

		Mockito.when(filter.duplicate(Mockito.anyInt())).thenReturn(filter);
		Mockito.when(filter.getFirstPipedStage()).thenReturn(sink);

		return filter;
	}

	private Sink mockSink() {

		Sink sink = Mockito.mock(Sink.class);

		Mockito.when(sink.duplicate(Mockito.anyInt())).thenReturn(sink);

		return sink;
	}
}