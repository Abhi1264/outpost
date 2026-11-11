import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Idempotent deposits against a running Outpost. Not part of {@code ./mvnw test}.
 * Eight accounts, so the threads do not queue on one row.
 */
public class PostingBench {

	static final int ACCOUNTS = 8;
	static final int THREADS = 8;
	static final int DEPOSITS = 400;

	public static void main(String[] args) throws Exception {
		String base = args.length > 0 ? args[0] : "http://127.0.0.1:8080";
		String auth = "Basic " + Base64.getEncoder().encodeToString("outpost:local-dev-secret".getBytes(StandardCharsets.UTF_8));
		HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

		String run = Long.toString(System.currentTimeMillis());
		int subscriptions = count(post(http, base, auth, "GET", "/api/v1/webhooks", null, null).body(), "\"id\"");
		List<String> accounts = new ArrayList<>();
		for (int i = 0; i < ACCOUNTS; i++) {
			HttpResponse<String> opened = post(http, base, auth, "POST", "/api/v1/accounts", "{\"currency\":\"USD\"}", null);
			String account = field(opened, "accountNumber");
			accounts.add(account);
			post(http, base, auth, "POST", "/api/v1/accounts/" + account + "/deposits", "{\"amount\":\"1.0000\"}", run + "-warm-" + i);
		}

		List<Long> ok = new ArrayList<>();
		List<String> errors = new ArrayList<>();
		long start = System.nanoTime();
		try (ExecutorService pool = Executors.newFixedThreadPool(THREADS)) {
			List<Future<Sample>> jobs = new ArrayList<>();
			for (int i = 0; i < DEPOSITS; i++) {
				String account = accounts.get(i % ACCOUNTS);
				String key = run + "-load-" + i;
				jobs.add(pool.submit(() -> {
					long t = System.nanoTime();
					HttpResponse<String> res = post(http, base, auth, "POST", "/api/v1/accounts/" + account + "/deposits",
							"{\"amount\":\"1.0000\"}", key);
					return new Sample(res.statusCode(), (System.nanoTime() - t) / 1_000_000, res.body());
				}));
			}
			for (Future<Sample> job : jobs) {
				Sample sample = job.get();
				if (sample.status == 201) {
					ok.add(sample.ms);
				}
				else {
					errors.add(sample.status + " " + sample.body);
				}
			}
		}
		double wall = (System.nanoTime() - start) / 1e9;
		if (ok.isEmpty()) {
			System.err.println(errors.get(0));
			System.exit(1);
		}
		ok.sort(Long::compareTo);
		System.out.printf(
				"subscriptions=%d processors=%d ok=%d err=%d wall_s=%.3f throughput=%.1f/s p50_ms=%d p99_ms=%d%n",
				subscriptions, Runtime.getRuntime().availableProcessors(), ok.size(), errors.size(), wall, ok.size() / wall,
				percentile(ok, 50), percentile(ok, 99));
		if (!errors.isEmpty()) {
			System.err.println(errors.get(0));
			System.exit(1);
		}
	}

	private static int percentile(List<Long> sorted, int p) {
		int index = (int) Math.round(p / 100.0 * (sorted.size() - 1));
		return sorted.get(index).intValue();
	}

	private static HttpResponse<String> post(HttpClient http, String base, String auth, String method, String path, String json,
			String key) throws Exception {
		HttpRequest.Builder req = HttpRequest.newBuilder(URI.create(base + path))
				.timeout(Duration.ofSeconds(30))
				.header("Authorization", auth);
		if (json != null) {
			req.header("Content-Type", "application/json").method(method, HttpRequest.BodyPublishers.ofString(json));
		}
		else {
			req.method(method, HttpRequest.BodyPublishers.noBody());
		}
		if (key != null) {
			req.header("Idempotency-Key", key);
		}
		return http.send(req.build(), HttpResponse.BodyHandlers.ofString());
	}

	private static String field(HttpResponse<String> res, String name) {
		if (res.statusCode() != 201) {
			throw new IllegalStateException(res.statusCode() + " " + res.body());
		}
		String needle = "\"" + name + "\":\"";
		int start = res.body().indexOf(needle);
		if (start < 0) {
			throw new IllegalStateException(res.body());
		}
		start += needle.length();
		return res.body().substring(start, res.body().indexOf('"', start));
	}

	private static int count(String body, String needle) {
		int n = 0;
		for (int i = 0; (i = body.indexOf(needle, i)) >= 0; i += needle.length()) {
			n++;
		}
		return n;
	}

	private record Sample(int status, long ms, String body) {
	}

}
