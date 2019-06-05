package org.nextprot.pipeline.statement.muxdemux;


import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.PipelineElement;
import org.nextprot.pipeline.statement.pipes.SinkPipe;
import org.nextprot.pipeline.statement.pipes.SourcePipe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * De-multiplexer receive statements via one input channel and
 * load balance them to multiple output channels.
 */
public class Demultiplexer implements PipelineElement, Runnable {

	private boolean hasStarted;

	private final int capacity;
	private final SinkPipe inputPort;
	private final CircularList<SourcePipe> outputPorts;
	private final List<PipelineElement> outputPipelineElements;

	public Demultiplexer(int capacity) {

		this.capacity = capacity;
		inputPort = new SinkPipe(capacity);
		outputPorts = new CircularList<>();
		outputPipelineElements = new ArrayList<>();
	}

	@Override
	public void connect(PipelineElement receiver) throws IOException {

		outputPipelineElements.add(receiver);
		SourcePipe outputPort = new SourcePipe();
		outputPort.connect(receiver.getSinkPipe());
		outputPorts.add(new SourcePipe());
	}

	@Override
	public void start(List<Thread> collector) {

		if (!hasStarted) {
			hasStarted = true;
			Thread thread = new Thread(this, getName());
			thread.start();
			collector.add(thread);
			System.out.println("Pipe "+getName()+": opened (capacity="+ capacity +")");
		}

		for (PipelineElement pipelineElement : outputPipelineElements) {
			pipelineElement.start(collector);
		}
	}

	@Override
	public void stop() throws IOException {


			for (PipelineElement outputPipelineElement : outputPipelineElements) {
				outputPipelineElement.getSourcePipe().close();
				System.out.println(outputPipelineElement.getName() + ": output port closed");
			}
		} catch (IOException e) {
			System.err.println(e.getMessage() + " in thread " + Thread.currentThread().getName());
		}
	}

	@Override
	public void run() {

		try {
			// 1. get input
			Statement[] buffer = new Statement[getCapacity()];

			int numOfStatements = inputPort.read(buffer, 0, getCapacity());

			int j=0;
			for (int i=0 ; i<numOfStatements ; i++) {

				// 2. split in n output batch
				// 3. distribute to all output
				outputPorts.get(j++).write(buffer[i]);

				System.out.println(Thread.currentThread().getName()
						+ ": filter statement "+ buffer[i].getStatementId());
			}
		}
		catch (IOException e) {
			System.err.println(e.getMessage() + " in thread " + Thread.currentThread().getName());
		}
		// When done with the data, close the pipe and flush the Writer
		finally {
			try {
				stop();
			} catch (IOException e) {
				System.err.println(Thread.currentThread().getName() + ": could not close the pipe, e="+e.getMessage());
			}
		}
	}

	public String getName() {
		return "Demux";
	}

	@Override
	public SinkPipe getSinkPipe() {
		return inputPort;
	}

	@Override
	public SourcePipe getSourcePipe() {

		return null;
	}

	@Override
	public int getCapacity() {
		return capacity;
	}

	public static class CircularList<E> extends ArrayList<E> {

		@Override
		public E get(int index) {
			return super.get(index % size());
		}
	}
}
