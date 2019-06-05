package org.nextprot.pipeline.statement;


import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.elements.BaseFilter;
import org.nextprot.pipeline.statement.ports.SinkPipePort;
import org.nextprot.pipeline.statement.ports.SourcePipePort;

import java.io.IOException;

/**
 * This filter just transmit statements from PipedInputPort to PipedOutputPort
 * and take a nap
 */
public class NarcolepticFilter extends BaseFilter {

	private static int COUNT = 0;

	private final int takeANapInMillis;
	private final int id;

	public NarcolepticFilter(int capacity) {

		this(capacity, -1);
	}

	public NarcolepticFilter(int capacity, int takeANapInMillis) {

		super(capacity);
		this.takeANapInMillis = takeANapInMillis;

		id = ++COUNT;
	}

	@Override
	public NarcolepticFilter duplicate(int capacity) {

		return new NarcolepticFilter(capacity);
	}

	@Override
	public String getName() {

		return getClass().getSimpleName()+"-"+id;
	}

	@Override
	public boolean filter(SinkPipePort in, SourcePipePort out) throws IOException {

		Statement[] buffer = new Statement[getCapacity()];

		int numOfStatements = in.read(buffer, 0, getCapacity());

		for (int i=0 ; i<numOfStatements ; i++) {

			out.write(buffer[i]);

			if (buffer[i] == END_OF_FLOW_TOKEN) {

				return true;
			} else {

				System.out.println(Thread.currentThread().getName()
						+ ": filter statement "+ buffer[i].getStatementId());
			}
		}

		if (takeANapInMillis > 0) {
			try {
				System.out.println(Thread.currentThread().getName()
						+ ": filter statement take a nap");
				Thread.sleep(takeANapInMillis);
			} catch (InterruptedException e) {
				System.err.println(e.getMessage());
			}
		}

		return false;
	}
}
