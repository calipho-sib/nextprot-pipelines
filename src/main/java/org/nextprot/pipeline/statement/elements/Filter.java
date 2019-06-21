package org.nextprot.pipeline.statement.elements;


import org.nextprot.commons.statements.Statement;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public interface Filter {

	/**
	 * Filter statements coming from input port to output port
	 *
	 * @param in  input port
	 * @param out output port
	 * @return true if the flow has ended
	 * @throws IOException
	 */
	boolean filter(BlockingQueue<Statement> in, BlockingQueue<Statement> out) throws Exception;
}
