package org.nextprot.pipeline.statement.elements.source.pump;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.elements.source.pump.WebStatementPump;

import java.io.IOException;
import java.net.URL;

public class WebStatementPumpTest {

	@Test
	public void pump() throws IOException {

		WebStatementPump pump = new WebStatementPump(mockURL());

		Assert.assertNotNull(pump.pump());
		Assert.assertNull(pump.pump());
	}

	@Test
	public void isNotEmpty() throws IOException {

		WebStatementPump pump = new WebStatementPump(mockURL());

		Assert.assertFalse(pump.isEmpty());
	}

	@Test
	public void isEmptyAfterOnePump() throws IOException {

		WebStatementPump pump = new WebStatementPump(mockURL());

		Assert.assertFalse(pump.isEmpty());
		Assert.assertTrue(pump.isEmpty());
	}

	public static URL mockURL() throws IOException {

		URL reader = Mockito.mock(URL.class);

		Statement statement = new Statement();

		/*Mockito.when(reader.nextStatement())
				.thenReturn(statement)
				.thenReturn(null);

		Mockito.when(reader.hasStatement())
				.thenReturn(false)
				.thenReturn(true);
*/
		return reader;
	}
}