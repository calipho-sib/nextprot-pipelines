package org.nextprot.pipeline.statement.nxflat;

import org.junit.Assert;
import org.junit.Test;

public class NXFlatDBTest {

	@Test(expected = IllegalArgumentException.class)
	public void shouldNotFindValueOfLowerCase() {

		NXFlatDB.valueOf("gnomad");
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldNotFindValueOfUpperCase() {

		NXFlatDB.valueOf("GNOMAD");
	}

	@Test
	public void shouldFindValueOfLowerCaseKey() {

		Assert.assertEquals(NXFlatDB.GnomAD, NXFlatDB.valueOfKey("gnomad"));
	}

	@Test
	public void shouldFindValueOfUpperCaseKey() {

		Assert.assertEquals(NXFlatDB.GnomAD, NXFlatDB.valueOfKey("GNOMAD"));
	}
}