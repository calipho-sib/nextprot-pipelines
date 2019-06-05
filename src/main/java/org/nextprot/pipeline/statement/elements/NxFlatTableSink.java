package org.nextprot.pipeline.statement.elements;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.PipelineElement;

import java.io.IOException;

public class NxFlatTableSink extends Sink {

	public enum Table {
		raw_statements,
		entry_mapped_statements
	}

	private final Table table;

	public NxFlatTableSink(Table table) {
		super(1);

		this.table = table;
	}

	@Override
	public String getName() {

		return "NxFlatSink";
	}

	@Override
	public PipelineElement duplicate(int capacity) {

		return new NxFlatTableSink(table);
	}

	@Override
	public void handleFlow() throws IOException {

		Statement statement;

		int i = 0;
		while ((statement = getSinkPipe().read()) != null) {
			System.out.println(Thread.currentThread().getName() + ": write statement " + statement.getStatementId()
					+ " in table " + table);
			i++;
		}
		System.out.println(Thread.currentThread().getName() + ": " + i + " statements evacuated");
	}
}