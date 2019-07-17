package org.nextprot.pipeline.statement.sources;


import org.apache.commons.io.IOUtils;
import org.nextprot.commons.statements.specs.Specifications;
import org.nextprot.commons.statements.specs.StatementSpecifications;
import org.nextprot.pipeline.statement.nxflat.source.pump.HttpStatementPump;
import sun.net.www.protocol.http.HttpURLConnection;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * A statement source proxy make HTTP requests to kant and produces multiple sources of Statements
 */
public class StatementSourceProxy {

	private static final Pattern JSON_LIST_PATTERN = Pattern.compile("href=\"(.*.json)\"", Pattern.MULTILINE);

	private final String sourceName;
	private final String hostName;
	private final String releaseDate;
	private final String homeStatementsURLString;
	private final StatementSpecifications specifications;
	private final ExecutorService executorService = Executors.newFixedThreadPool(10);

	private StatementSourceProxy(String sourceName, String releaseDate, StatementSpecifications specifications) throws IOException {

		this("http://kant.sib.swiss:9001", sourceName, releaseDate, specifications);
	}

	private StatementSourceProxy(String hostName, String sourceName, String releaseDate, StatementSpecifications specifications) throws IOException {

		if (isServerDown(hostName)) {
			throw new IllegalArgumentException("Cannot connect to the statement source " + sourceName + " at host " + hostName
					+ ": service is down");
		}
		this.sourceName = sourceName;
		this.hostName = hostName;
		this.releaseDate = releaseDate;

		this.homeStatementsURLString = homeStatementsURL();
		this.specifications = specifications;

		if (isServerDown(homeStatementsURLString)) {
			throw new IllegalArgumentException("Cannot get statements from the source " + sourceName + " at unknown release date '" + releaseDate + "'");
		}
	}

	public static StatementSourceProxy BioEditor(String releaseDate) throws IOException {

		return new StatementSourceProxy("BioEditor", releaseDate, new Specifications.Builder().build());
	}

	public static StatementSourceProxy GlyConnect(String releaseDate) throws IOException {

		return new StatementSourceProxy("GlyConnect", releaseDate, new Specifications.Builder().build());
	}

	public static StatementSourceProxy GnomAD(String releaseDate) throws IOException {

		return new StatementSourceProxy("gnomAD", releaseDate, new Specifications.Builder()
				.withExtraFields(Arrays.asList("CANONICAL", "ALLELE_COUNT", "ALLELE_SAMPLED"))
				.withExtraFieldsContributingToUnicityKey(Collections.singletonList("DBSNP_ID"))
				.build());
	}

	public static StatementSourceProxy valueOf(String sourceName, String releaseDate) throws IOException {

		switch (sourceName.toLowerCase()) {
			case "bioeditor":
				return BioEditor(releaseDate);
			case "glyconnect":
				return GlyConnect(releaseDate);
			case "gnomad":
				return GnomAD(releaseDate);
			default:
				throw new IllegalArgumentException("unknown source name "+sourceName);
		}
	}

	public void submit() {

		createPumps().forEach(pump -> {
			executorService.submit(() -> {

				//pump

				Thread.sleep(1000);
				return null;
			});
		});
	}

	Stream<HttpStatementPump> createPumps() {

		return extractAllJsonUrls().stream()
				.map(url -> new HttpStatementPump(url, specifications));
	}

	private String homeStatementsURL() {

		return hostName + "/" + sourceName.toLowerCase() + "/" + releaseDate;
	}

	private List<String> extractAllJsonUrls() {

		List<String> allJsonUrls = new ArrayList<>();

		try {
			URLConnection connection = new URL(homeStatementsURLString).openConnection();
			String content = IOUtils.toString(connection.getInputStream(), "UTF8");

			Matcher matcher = JSON_LIST_PATTERN.matcher(content);
			while (matcher.find()) {
				allJsonUrls.add(homeStatementsURLString + "/" + matcher.group(1));
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		return allJsonUrls;
	}

	private static boolean isServerDown(String url) throws IOException {

		HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
		connection.setRequestMethod("HEAD");
		connection.setConnectTimeout(3000);
		connection.setReadTimeout(3000);

		try {
			connection.connect();

			return connection.getResponseCode() != HttpURLConnection.HTTP_OK;
		} catch (IOException e) {

			throw new IOException("statement service " + url + " does not respond: " + e.getMessage());
		} finally {

			connection.disconnect();
		}
	}
}
