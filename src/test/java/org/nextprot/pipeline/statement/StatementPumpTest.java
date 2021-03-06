package org.nextprot.pipeline.statement;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.elements.Source;

import java.io.IOException;
import java.io.Reader;

public class StatementPumpTest {

	@Test
	public void pump() throws IOException {

		Source.StatementPump pump = new Source.StatementPump(mockReader());

		Assert.assertNotNull(pump.pump());
		Assert.assertNull(pump.pump());
	}

	@Test
	public void isNotEmpty() throws IOException {

		Source.StatementPump pump = new Source.StatementPump(mockReader());

		Assert.assertFalse(pump.isEmpty());
	}

	@Test
	public void isEmptyAfterOnePump() throws IOException {

		Source.StatementPump pump = new Source.StatementPump(mockReader());

		Assert.assertFalse(pump.isEmpty());
		Assert.assertTrue(pump.isEmpty());
	}

	public static Reader mockReader() throws IOException {

		Reader reader = Mockito.mock(Reader.class);

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