package org.nextprot.pipeline.statement.elements;

import org.nextprot.commons.statements.Statement;

import java.io.IOException;

public class NxFlatTableSink extends Sink {

	private static int COUNT = 0;

	public enum Table {
		raw_statements,
		entry_mapped_statements
	}

	private final Table table;
	private final int id;

	public NxFlatTableSink(Table table) {
		super(1);

		this.table = table;
		id = ++COUNT;
	}

	@Override
	public String getThreadName() {

		return getClass().getSimpleName()+"-"+id;
	}

	@Override
	public NxFlatTableSink duplicate(int capacity) {

		return new NxFlatTableSink(table);
	}

	@Override
	public void handleFlow() throws IOException {

		Statement statement;

		int i = 0;
		while ((statement = getSinkPipePort().read()) != END_OF_FLOW_TOKEN) {
			printlnTextInLog("write statement " + statement.getStatementId() + " in table " + table);
			i++;
		}
		printlnTextInLog(i + " statements evacuated");
	}
}