package org.nextprot.pipeline.statement.elements.runnable;


import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.PipelineElement;

import java.io.FileNotFoundException;

public interface FlowablePipelineElement<E extends PipelineElement> extends Runnable {

	/** poisoned statement pill */
	Statement POISONED_STATEMENT = new Statement();

	/** handle the current flow and @return true if the flow has been poisoned */
	boolean handleFlow(E pipelineElement) throws Exception;

	/** @return the name of thread handling this flow */
	String getThreadName();

	FlowEventHandler createEventHandler() throws FileNotFoundException;
}
