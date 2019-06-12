package org.nextprot.pipeline.statement.elements;

import org.nextprot.commons.statements.Statement;

import java.io.IOException;
import java.util.List;

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
	public boolean handleFlow(List<Statement> buffer) throws IOException {

		Statement statement = getSinkPipePort().read();
		statementsHandled(1);
		return statement == END_OF_FLOW_TOKEN;
	}

	@Override
	public void elementOpened(int capacity) {

	}

	@Override
	public void endOfFlow() {
		//printlnTextInLog(i + " statements evacuated");
	}

	@Override
	public void statementsHandled(int statements) {
		//printlnTextInLog("write statement " + statement.getStatementId() + " in table " + table);
	}
}