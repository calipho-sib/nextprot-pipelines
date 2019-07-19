package org.nextprot.pipeline.statement.core.stage;


import org.nextprot.commons.statements.Statement;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public interface Filter {

	/**
	 * Filter statements coming from input channel to output channel
	 *
	 * @param in  input channel
	 * @param out output channel
	 * @return true if the flow has ended
	 * @throws IOException
	 */
	boolean filter(BlockingQueue<Statement> in, BlockingQueue<Statement> out) throws Exception;
}
