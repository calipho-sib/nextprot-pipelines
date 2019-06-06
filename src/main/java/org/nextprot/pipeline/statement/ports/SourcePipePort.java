package org.nextprot.pipeline.statement.ports;

import org.nextprot.commons.statements.Statement;

import java.io.IOException;
import java.util.List;

/**
 * Piped statement-output streams.
 *
 * Note: this class is a variation of PipedWriter, it emit Statements instead of chars
 */
public class SourcePipePort {

	private final int capacity;
	private SinkPipePort sink;

	private boolean closed = false;

	public SourcePipePort(int capacity) {

		this.capacity = capacity;
	}

	public synchronized void connect(SinkPipePort snk) throws IOException {

		if (snk == null) {
			throw new NullPointerException();
		} else if (sink != null || snk.isConnected()) {
			throw new IOException("Already connected");
		} else if (snk.isClosedByReader() || closed) {
			throw new IOException("Pipe closed");
		}

		sink = snk;
		snk.setIn(-1);
		snk.setOut(0);
		snk.setConnected(true);
	}

	public synchronized void disconnectSink() {

		if (sink != null) {

			sink.setConnected(false);
			sink = null;
		}
	}

	/**
	 * Writes the specified <code>statement</code> to the piped output stream.
	 * If a thread was reading data statements from the connected piped input
	 * stream, but the thread is no longer alive, then an
	 * <code>IOException</code> is thrown.
	 * <p>
	 * Implements the <code>write</code> method of <code>Writer</code>.
	 *
	 * @param      statement  the <code>statement</code> to be written.
	 * @exception  IOException  if the pipe is
	 *          <a href=PipedOutputStream.html#BROKEN> <code>broken</code></a>,
	 *          {@link #connect(SinkPipePort) unconnected}, closed
	 *          or an I/O error occurs.
	 */
	public void write(Statement statement)  throws IOException {
		if (sink == null) {
			throw new IOException("Pipe not connected");
		}
		sink.receive(statement);
	}

	/**
	 * Writes <code>len</code> statements from the specified statement array
	 * starting at offset <code>off</code> to this piped output stream.
	 * This method blocks until all the characters are written to the output
	 * stream.
	 * If a thread was reading data statements from the connected piped input
	 * stream, but the thread is no longer alive, then an
	 * <code>IOException</code> is thrown.
	 *
	 * @param      buffer the data.
	 * @param      offset the start offset in the data.
	 * @param      len   the number of statements to write (with a maximum capacity)
	 * @exception  IOException  if the pipe is
	 *          <a href=PipedOutputStream.html#BROKEN> <code>broken</code></a>,
	 *          {@link #connect(SinkPipePort) unconnected}, closed
	 *          or an I/O error occurs.
	 */
	public void write(List<Statement> buffer, int offset, int len) throws IOException {

		int length = (len < capacity) ? len : capacity;

		if (sink == null) {
			throw new IOException("Pipe not connected");
		} else if ((offset | length | (offset + length) | (buffer.size() - (offset + length))) < 0) {
			throw new IndexOutOfBoundsException();
		}
		sink.receive(buffer, offset, length);
	}

	/**
	 * Flushes this output stream and forces any buffered output statements
	 * to be written out.
	 * This will notify any readers that statements are waiting in the pipe.
	 *
	 * @exception  IOException  if the pipe is closed, or an I/O error occurs.
	 */
	public synchronized void flush() throws IOException {
		if (sink != null) {
			if (sink.isClosedByReader() || closed) {
				throw new IOException("Pipe closed");
			}
			synchronized (sink) {
				sink.notifyAll();
			}
		}
	}

	public boolean isConnected() {

		return sink != null;
	}

	/**
	 * Closes this piped output stream and releases any system resources
	 * associated with this stream. This stream may no longer be used for
	 * writing statements.
	 *
	 * @exception  IOException  if an I/O error occurs.
	 */
	public void close()  throws IOException {
		closed = true;
		if (sink != null) {
			sink.receivedLast();
		}
	}

	public int capacity() {
		return capacity;
	}
}
