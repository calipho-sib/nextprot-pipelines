package org.nextprot.pipeline.statement.elements.source.pump;

import org.nextprot.commons.statements.Statement;
import org.nextprot.commons.statements.reader.BufferableStatementReader;
import org.nextprot.commons.statements.reader.BufferedJsonStatementReader;
import org.nextprot.pipeline.statement.elements.source.Pump;
import sun.net.www.protocol.http.HttpURLConnection;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.List;

public class WebStatementPump implements Pump<Statement> {

	private final BufferableStatementReader reader;
	private final int capacity;

	public WebStatementPump(URL url) throws IOException {

		this(url, 100);
	}

	public WebStatementPump(URL url, int capacity) throws IOException {

		if (!isServiceUp(url)) {
			throw new IOException("Cannot create a pump: " + url + " is not reachable");
		}

		this.capacity = capacity;
		this.reader = new BufferedJsonStatementReader(new InputStreamReader(url.openStream()), capacity);
	}

	private static boolean isServiceUp(URL url) throws IOException {

		HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		connection.setRequestMethod("HEAD");
		connection.setConnectTimeout(3000);
		connection.setReadTimeout(3000);

		try {
			connection.connect();

			return connection.getResponseCode() == HttpURLConnection.HTTP_OK;
		} catch (IOException e) {

			throw new IOException("statement service " + url + " does not respond: " + e.getMessage());
		} finally {

			connection.disconnect();
		}
	}

	@Override
	public Statement pump() throws IOException {

		return reader.nextStatement();
	}

	@Override
	public int capacity() {

		return capacity;
	}

	@Override
	public int pump(List<Statement> collector) throws IOException {

		return reader.readStatements(collector);
	}

	@Override
	public boolean isEmpty() throws IOException {

		return reader.hasStatement();
	}

	@Override
	public void stop() throws IOException {

		reader.close();
	}
}
