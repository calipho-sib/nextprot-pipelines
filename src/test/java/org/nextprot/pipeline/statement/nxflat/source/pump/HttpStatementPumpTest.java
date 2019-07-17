package org.nextprot.pipeline.statement.nxflat.source.pump;

import org.junit.Assert;
import org.junit.Test;

public class HttpStatementPumpTest {

	@Test
	public void sourceShouldBeEmptyWhenNotConnected() {

		HttpStatementPump pump = new HttpStatementPump("");

	}

	@Test
	public void activatedPumpShouldBeEmpty() {

		HttpStatementPump pump = new HttpStatementPump("http://kant.sib.swiss:9001/glyconnect/2019-01-22/all-entries.json");

		Assert.assertTrue(pump.isSourceEmpty());
	}

	@Test
	public void pumpShouldBeEmpty2() {

		HttpStatementPump pump = new HttpStatementPump("http://kant.sib.swiss:9001/glyconnect/2019-01-22/all-entries.json");

		pump.pump();
		Assert.assertTrue(pump.isSourceEmpty());
	}
}