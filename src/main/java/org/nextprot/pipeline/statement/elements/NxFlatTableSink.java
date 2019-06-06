package org.nextprot.pipeline.statement.elements;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.PipelineElement;

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
	public String getName() {

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
			System.out.println(Thread.currentThread().getName() + ": write statement " + statement.getStatementId()
					+ " in table " + table);
			i++;
		}
		System.out.println(Thread.currentThread().getName() + ": " + i + " statements evacuated");
	}
}