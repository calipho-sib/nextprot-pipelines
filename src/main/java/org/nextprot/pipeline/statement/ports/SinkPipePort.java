package org.nextprot.pipeline.statement.ports;

import org.nextprot.commons.statements.Statement;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;

/**
 * Piped statement-input streams.
 *
 * Note: this class is a variation of PipedReader, it collect Statements instead of chars
 */
public class SinkPipePort {

	private boolean closedByWriter = false;
	private boolean closedByReader = false;
	private boolean connected = false;

	/* Communicating threads (pipeline element connected to the sink and source ports) */
	private Thread readSide;
	private Thread writeSide;

	/**
	 * The circular buffer into which incoming data is placed.
	 */
	private Statement[] buffer;

	/**
	 * The index of the position in the circular buffer at which the
	 * next character of data will be stored when received from the connected
	 * piped writer. <code>in&lt;0</code> implies the buffer is empty,
	 * <code>in==out</code> implies the buffer is full
	 */
	private int in = -1;

	/**
	 * The index of the position in the circular buffer at which the next
	 * character of data will be read by this piped reader.
	 */
	private int out = 0;

	public SinkPipePort(int capacity) {
		init(capacity);
	}

	private void init(int capacity) {
		if (capacity <= 0) {
			throw new IllegalArgumentException("Pipe size <= 0");
		}
		buffer = new Statement[capacity];
	}

	public int capacity() {

		return buffer.length;
	}

	/**
	 * Receives a statement. This method will block if no input is
	 * available.
	 */
	synchronized void receive(Statement statement) throws IOException {

		if (!connected) {
			throw new IOException("Pipe not connected");
		} else if (closedByWriter || closedByReader) {
			throw new IOException("Pipe closed");
		} else if (readSide != null && !readSide.isAlive()) {
			throw new IOException("Read end dead");
		}

		writeSide = Thread.currentThread();
		while (in == out) {
			if ((readSide != null) && !readSide.isAlive()) {
				throw new IOException("Pipe broken");
			}
			/* full: kick any waiting readers */
			notifyAll();
			try {
				wait(1000);
			} catch (InterruptedException ex) {
				throw new InterruptedIOException();
			}
		}
		if (in < 0) {
			in = 0;
			out = 0;
		}
		buffer[in++] = statement;
		if (in >= buffer.length) {
			in = 0;
		}
	}

	synchronized void receive(List<Statement> statements, int off, int len)  throws IOException {
		while (--len >= 0) {
			receive(statements.get(off++));
		}
	}

	/**
	 * Notifies all waiting threads that the last statement has been
	 * received.
	 */
	synchronized void receivedLast() {
		closedByWriter = true;
		notifyAll();
	}

	/**
	 * Reads the next statement from this piped stream.
	 * If no statement is available because the end of the stream
	 * has been reached, the value <code>null</code> is returned.
	 * This method blocks until input data is available, the end of
	 * the stream is detected, or an exception is thrown.
	 *
	 * @return     the next statement, or <code>null</code> if the end of the
	 *             stream is reached.
	 * @exception  IOException  if the pipe is
	 *          <code>broken</code></a>, PipedOutputPort unconnected}, closed or an I/O error occurs.
	 */
	public synchronized Statement read() throws IOException {

		checkPipes();

		readSide = Thread.currentThread();
		int trials = 2;
		while (in < 0) {
			if (closedByWriter) {
				/* closed by writer, return EOF */
				return null;
			}
			if ((writeSide != null) && (!writeSide.isAlive()) && (--trials < 0)) {
				throw new IOException("Pipe broken");
			}
			/* might be a writer waiting */
			notifyAll();
			try {
				wait(1000);
			} catch (InterruptedException ex) {
				throw new InterruptedIOException();
			}
		}
		Statement ret = buffer[out++];
		if (out >= buffer.length) {
			out = 0;
		}
		if (in == out) {
			/* now empty */
			in = -1;
		}
		return ret;
	}

	/**
	 * Reads up to <code>len</code> statements from this piped
	 * stream into an array of statements. Less than <code>len</code> statements
	 * will be read if the end of the data stream is reached or if
	 * <code>len</code> exceeds the pipe's buffer size. This method
	 * blocks until at least one statement of input is available.
	 *
	 * @param      sbuf  the buffer into which the data is read.
	 * @param      off   the start offset of the data.
	 * @param      len   the maximum number of statements read (maximum length capacity).
	 * @return     the total number of statements read into the buffer, or
	 *             <code>-1</code> if there is no more data because the end of
	 *             the stream has been reached.
	 * @exception  IOException  if the pipe is
	 *                  <a href=PipedInputStream.html#BROKEN> <code>broken</code></a>,
	 *                  PipedOutputPort unconnected}, closed, or an I/O error occurs.
	 */
	public synchronized int read(Statement[] sbuf, int off, int len)  throws IOException {

		int length = (len < buffer.length) ? len : buffer.length;

		checkPipes();

		if ((off < 0) || (off > sbuf.length) || (length < 0) ||
				((off + length) > sbuf.length) || ((off + length) < 0)) {
			throw new IndexOutOfBoundsException();
		} else if (length == 0) {
			return 0;
		}

		/* possibly wait on the first statement */
		Statement statement = read();
		if (statement == null) {
			return -1;
		}
		sbuf[off] =  statement;
		int rlen = 1;
		while ((in >= 0) && (--length > 0)) {
			sbuf[off + rlen] = buffer[out++];
			rlen++;
			if (out >= buffer.length) {
				out = 0;
			}
			if (in == out) {
				/* now empty */
				in = -1;
			}
		}
		return rlen;
	}

	/**
	 * Tell whether this stream is ready to be read.  A piped statement
	 * stream is ready if the circular buffer is not empty.
	 *
	 * @exception  IOException  if the pipe is
	 *                  <a href=PipedInputStream.html#BROKEN> <code>broken</code></a>,
	 *                  PipedOutputPort unconnected}, or closed.
	 */
	public synchronized boolean ready() throws IOException {

		checkPipes();

		if (in < 0) {
			return false;
		} else {
			return true;
		}
	}

	/**
	 * Closes this piped stream and releases any system resources
	 * associated with the stream.
	 *
	 * @exception  IOException  if an I/O error occurs.
	 */
	public void close()  throws IOException {
		in = -1;
		closedByReader = true;
	}

	public boolean isConnected() {
		return connected;
	}

	public boolean isClosedByReader() {
		return closedByReader;
	}

	public void setIn(int in) {
		this.in = in;
	}

	public void setOut(int out) {
		this.out = out;
	}

	public void setConnected(boolean connected) {
		this.connected = connected;
	}

	private void checkPipes() throws IOException {

		if (!connected) {
			throw new IOException("Pipe not connected");
		} else if (closedByReader) {
			throw new IOException("Pipe closed");
		} else if (writeSide != null && !writeSide.isAlive()
				&& !closedByWriter && (in < 0)) {
			throw new IOException("Write end dead");
		}
	}
}
