package org.nextprot.pipeline.statement.elements.runnable;

import org.nextprot.commons.statements.Statement;

import java.io.IOException;
import java.util.List;

public interface RunnablePipelineElement extends FlowEventHandler, Runnable {

	/** handle the current flow and @return true if the flow ends */
	boolean handleFlow(List<Statement> buffer) throws IOException;

	/** @return the name of thread handling this flow */
	String getThreadName();

	int capacity();
}
