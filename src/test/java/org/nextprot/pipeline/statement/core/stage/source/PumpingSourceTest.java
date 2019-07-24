package org.nextprot.pipeline.statement.core.stage.source;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.nextprot.commons.statements.Statement;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.*;
import static org.mockito.Mockito.times;

public class PumpingSourceTest {

	@Test
	public void extractShouldMakePumpPumpsOnce() {

		Pump<Statement> pump = mockPump();
		PumpingSource source = new PumpingSource(pump,10);
		source.extract();

		Mockito.verify(pump, times(1)).pump();
		Assert.assertEquals(10, source.extractionCapacity());
	}

	@Test(expected = IllegalAccessError.class)
	public void getSinkChannelShouldThrowError() {

		PumpingSource source = new PumpingSource(mockPump(),10);
		source.getSinkChannel();
	}

	@Test(expected = IllegalAccessError.class)
	public void setSinkChannelShouldThrowError() {

		PumpingSource source = new PumpingSource(mockPump(),10);
		source.setSinkChannel(new ArrayBlockingQueue<>(10));
	}

	private Pump<Statement> mockPump() {

		Pump<Statement> pump = Mockito.mock(Pump.class);
		return pump;
	}
}