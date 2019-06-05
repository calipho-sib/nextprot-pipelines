package org.nextprot.pipeline.statement.pipes;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.Pipe;
import org.nextprot.pipeline.statement.ports.PipedInputPort;
import org.nextprot.pipeline.statement.ports.PipedOutputPort;

import java.io.IOException;
import java.util.List;

/**
 * A Pipe is a kind of thread that is connectable to another thread,
 * known as its "receiver". If so, it creates a PipedOutputPort stream
 * through which it can write statements to that receiver.
 *
 * It connects its PipedOutputPort stream to a corresponding PipedInputPort
 * stream in the receiver.
 **/
public abstract class BasePipe implements Pipe, Runnable {

	public static final Statement END_OF_FLOW_TOKEN = null;

	private boolean hasStarted;

	private final int capacity;
	protected PipedOutputPort outputPort;
	protected PipedInputPort inputPort;

	private Pipe receiver = null;

	public BasePipe(int capacity) {
		this.capacity = capacity;
	}

	public BasePipe(int capacity, PipedInputPort inputPort) {
		this(capacity);
		this.inputPort = inputPort;
	}

	/**
	 * Connect this pipe with the receiver pipe
	 * @param receiver
	 * @throws IOException
	 */
	@Override
	public void connect(Pipe receiver) throws IOException {

		this.receiver = receiver;
		outputPort = new PipedOutputPort();
		outputPort.connect(receiver.getInputPort());
	}

	@Override
	public int getCapacity() {

		return capacity;
	}

	/**
	 * This protected method requests a Pipe threads to create and return
	 * a PipedInputPort thread so that another Pipe thread can connect to it.
	 **/
	@Override
	public PipedInputPort getInputPort() {

		return inputPort;
	}

	@Override
	public PipedOutputPort getOutputPort() {

		return outputPort;
	}

	public void openPipe(List<Thread> collector) {

		if (!hasStarted) {
			hasStarted = true;
			Thread thread = new Thread(this, getName());
			thread.start();
			collector.add(thread);
			System.out.println("Pipe "+getName()+": opened (capacity="+ capacity +")");
		}

		if (receiver != null) {
			receiver.openPipe(collector);
		}
	}

	@Override
	public void run() {

		try {
			handleFlow();
			endOfFlow();
		}
		catch (IOException e) {
			System.err.println(e.getMessage() + " in thread " + Thread.currentThread().getName());
		}
		// When done with the data, close the pipe and flush the Writer
		finally {
			try {
				closePipe();
			} catch (IOException e) {
				System.err.println(Thread.currentThread().getName() + ": could not close the pipe, e="+e.getMessage());
			}
		}
	}

	protected void endOfFlow() {

		System.out.println(Thread.currentThread().getName() + ": end of flow");
	}

	public void closePipe() throws IOException {

		try {
			if (inputPort != null) {
				inputPort.close();
				System.out.println(Thread.currentThread().getName() + ": input port closed");
			}
			if (outputPort != null) {
				outputPort.close();
				System.out.println(Thread.currentThread().getName() + ": output port closed");
			}
		} catch (IOException e) {
			System.err.println(e.getMessage() + " in thread " + Thread.currentThread().getName());
		}
	}

	protected abstract void handleFlow() throws IOException;
}
