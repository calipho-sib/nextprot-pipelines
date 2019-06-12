package org.nextprot.pipeline.statement.elements;

import org.nextprot.pipeline.statement.PipelineElement;
import org.nextprot.pipeline.statement.ports.SinkPipePort;
import org.nextprot.pipeline.statement.ports.SourcePipePort;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.function.Supplier;

public abstract class LoggedElement implements PipelineElement<PipelineElement>, EventHandler {

	private final BasePipelineElement pipelineElement;
	private final ThreadLocal<PrintStream> localEventHandler;
	private final Supplier<PrintStream> printStreamConstr;

	public LoggedElement(BasePipelineElement pipelineElement, Supplier<PrintStream> printStreamConstr) {

		this.pipelineElement = pipelineElement;
		this.printStreamConstr = printStreamConstr;
		this.localEventHandler = new ThreadLocal<>();
	}

	@Override
	public String getThreadName() {
		return pipelineElement.getThreadName();
	}

	@Override
	public void pipe(PipelineElement nextElement) throws IOException {
		pipelineElement.pipe(nextElement);
	}

	@Override
	public SinkPipePort getSinkPipePort() {
		return pipelineElement.getSinkPipePort();
	}

	@Override
	public SourcePipePort getSourcePipePort() {
		return pipelineElement.getSourcePipePort();
	}

	@Override
	public PipelineElement nextElement() {
		return pipelineElement.nextElement();
	}

	@Override
	public void start(List<Thread> collector) {
		pipelineElement.start(collector);
		localEventHandler.set(printStreamConstr.get());
	}

	@Override
	public void stop() throws IOException {
		pipelineElement.stop();
	}

	@Override
	public void run() {
		pipelineElement.run();
	}

	//@Override
	public void pipeElementOpened(int capacity) {
		sendMessage("opened (capacity=" + capacity + ")");
	}

	@Override
	public void endOfFlow() {
		sendMessage("end of flow");
	}

	@Override
	public void sinkPipePortClosed() {
		sendMessage("sink pipe port closed");
	}

	@Override
	public void sourcePipePortClosed() {
		sendMessage("source pipe port closed");
	}

	//@Override
	public void pipeElementClosed() {
		sendMessage("closed");
	}

	protected void sendMessage(String message) {

		localEventHandler.get().println(getThreadName()+":" + message);
	}
}
