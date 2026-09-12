package com.ledger;

import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import com.ledger.ledger.LedgerApi.AccountView;
import com.ledger.ledger.LedgerApi.TransactionView;
import com.ledger.webhook.HmacSigner;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.ConsumerFactory;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class WebhookDispatchTest extends IntegrationTestBase {

	record Subscription(UUID id, String secret, String path) {
	}

	@Autowired
	ConsumerFactory<String, String> consumerFactory;

	@Autowired
	JsonMapper json;

	@Value("${outpost.webhook.dlq-topic}")
	String dlqTopic;

	private final List<UUID> created = new ArrayList<>();

	@AfterEach
	void deactivateSubscriptions() {
		created.forEach(id -> api.delete().uri("/api/v1/webhooks/{id}", id).retrieve().toBodilessEntity());
		WIREMOCK.resetAll();
	}

	private Subscription subscribe(String path) {
		JsonNode body = api.post().uri("/api/v1/webhooks").contentType(MediaType.APPLICATION_JSON)
				.body(Map.of("targetUrl", WIREMOCK.baseUrl() + path)).retrieve().body(JsonNode.class);
		UUID id = UUID.fromString(body.get("id").asString());
		created.add(id);
		return new Subscription(id, body.get("secret").asString(), path);
	}

	private String outboxStatus(String path) {
		return jdbc.sql("select status from webhook_outbox where target_url = ?").param(WIREMOCK.baseUrl() + path)
				.query(String.class).single();
	}

	private int outboxRetryCount(String path) {
		return jdbc.sql("select retry_count from webhook_outbox where target_url = ?").param(WIREMOCK.baseUrl() + path)
				.query(Integer.class).single();
	}

	@Test
	void deliversSignedPayloadExactlyOnce() {
		Subscription sub = subscribe("/hooks/ok-" + UUID.randomUUID());
		WIREMOCK.stubFor(post(urlEqualTo(sub.path())).willReturn(aResponse().withStatus(200)));

		AccountView account = openAccount("USD");
		TransactionView tx = deposit(account.accountNumber(), money("12.3400")).getBody();

		await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(outboxStatus(sub.path())).isEqualTo("DELIVERED"));
		// poller keeps re-publishing for a while; duplicates must not cause a second HTTP call
		await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(3))
				.untilAsserted(() -> assertThat(WIREMOCK.findAll(postRequestedFor(urlEqualTo(sub.path())))).hasSize(1));

		LoggedRequest received = WIREMOCK.findAll(postRequestedFor(urlEqualTo(sub.path()))).getFirst();
		String rawBody = received.getBodyAsString();
		assertThat(received.getHeader("X-Signature-256")).isEqualTo(HmacSigner.sign(rawBody, sub.secret()));
		assertThat(received.getHeader("X-Webhook-Event")).isEqualTo("transaction.completed");
		assertThat(received.getHeader("X-Webhook-Attempt")).isEqualTo("1");
		assertThat(received.getHeader("Content-Type")).startsWith(MediaType.APPLICATION_JSON_VALUE);

		JsonNode payload = json.readTree(rawBody);
		assertThat(payload.get("eventType").asString()).isEqualTo("transaction.completed");
		assertThat(payload.get("data").get("transactionId").asString()).isEqualTo(tx.transactionId().toString());
		assertThat(payload.get("data").get("amount").decimalValue()).isEqualByComparingTo(money("12.3400"));
		assertThat(payload.get("data").get("entries")).hasSize(2);
		assertThat(outboxRetryCount(sub.path())).isZero();
	}

	@Test
	void signatureDoesNotVerifyWithTheWrongSecret() {
		Subscription sub = subscribe("/hooks/sig-" + UUID.randomUUID());
		WIREMOCK.stubFor(post(urlEqualTo(sub.path())).willReturn(aResponse().withStatus(204)));

		AccountView account = openAccount("USD");
		deposit(account.accountNumber(), money("1.0000"));
		await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(outboxStatus(sub.path())).isEqualTo("DELIVERED"));

		LoggedRequest received = WIREMOCK.findAll(postRequestedFor(urlEqualTo(sub.path()))).getFirst();
		assertThat(received.getHeader("X-Signature-256")).startsWith("sha256=").hasSize(7 + 64)
				.isNotEqualTo(HmacSigner.sign(received.getBodyAsString(), "wrong-secret"));
	}

	@Test
	void retriesWithBackoffUntilTheEndpointRecovers() {
		Subscription sub = subscribe("/hooks/flaky-" + UUID.randomUUID());
		String scenario = "flaky-" + sub.id();
		WIREMOCK.stubFor(post(urlEqualTo(sub.path())).inScenario(scenario).whenScenarioStateIs(STARTED)
				.willReturn(aResponse().withStatus(503)).willSetStateTo("second"));
		WIREMOCK.stubFor(post(urlEqualTo(sub.path())).inScenario(scenario).whenScenarioStateIs("second")
				.willReturn(aResponse().withStatus(500)).willSetStateTo("third"));
		WIREMOCK.stubFor(post(urlEqualTo(sub.path())).inScenario(scenario).whenScenarioStateIs("third")
				.willReturn(aResponse().withStatus(200)));

		AccountView account = openAccount("USD");
		deposit(account.accountNumber(), money("3.0000"));

		await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(outboxStatus(sub.path())).isEqualTo("DELIVERED"));
		List<LoggedRequest> requests = WIREMOCK.findAll(postRequestedFor(urlEqualTo(sub.path())));
		assertThat(requests).hasSize(3);
		assertThat(requests).extracting(r -> r.getHeader("X-Webhook-Attempt")).containsExactly("1", "2", "3");
		assertThat(requests).extracting(LoggedRequest::getBodyAsString).containsOnly(requests.getFirst().getBodyAsString());
		assertThat(outboxRetryCount(sub.path())).isEqualTo(2);
	}

	@Test
	void exhaustedRetriesMarkFailedAndPublishToDeadLetterTopic() {
		Subscription sub = subscribe("/hooks/dead-" + UUID.randomUUID());
		WIREMOCK.stubFor(post(urlEqualTo(sub.path())).willReturn(aResponse().withStatus(500)));

		AccountView account = openAccount("USD");
		deposit(account.accountNumber(), money("9.0000"));

		await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(outboxStatus(sub.path())).isEqualTo("FAILED"));
		assertThat(WIREMOCK.findAll(postRequestedFor(urlEqualTo(sub.path())))).hasSize(4);
		assertThat(outboxRetryCount(sub.path())).isEqualTo(4);
		UUID outboxId = jdbc.sql("select id from webhook_outbox where target_url = ?").param(WIREMOCK.baseUrl() + sub.path())
				.query(UUID.class).single();
		assertThat(jdbc.sql("select last_error from webhook_outbox where id = ?").param(outboxId).query(String.class).single())
				.contains("500");

		try (Consumer<String, String> consumer = consumerFactory.createConsumer("dlq-test-" + UUID.randomUUID(), "")) {
			consumer.subscribe(List.of(dlqTopic));
			List<ConsumerRecord<String, String>> matches = new ArrayList<>();
			await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
				consumer.poll(Duration.ofMillis(500)).forEach(r -> {
					if (r.key().equals(outboxId.toString())) {
						matches.add(r);
					}
				});
				assertThat(matches).isNotEmpty();
			});
			JsonNode dead = json.readTree(matches.getFirst().value());
			assertThat(dead.get("outboxId").asString()).isEqualTo(outboxId.toString());
			assertThat(dead.get("attempts").asInt()).isEqualTo(4);
			assertThat(dead.get("targetUrl").asString()).isEqualTo(WIREMOCK.baseUrl() + sub.path());
			assertThat(dead.get("payload").get("eventType").asString()).isEqualTo("transaction.completed");
			assertThat(dead.has("secretKey")).isFalse();
		}
	}

	@Test
	void clientErrorsFailImmediatelyWithoutRetry() {
		Subscription sub = subscribe("/hooks/gone-" + UUID.randomUUID());
		WIREMOCK.stubFor(post(urlEqualTo(sub.path())).willReturn(aResponse().withStatus(410)));

		AccountView account = openAccount("USD");
		deposit(account.accountNumber(), money("2.0000"));

		await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(outboxStatus(sub.path())).isEqualTo("FAILED"));
		await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(3))
				.untilAsserted(() -> assertThat(WIREMOCK.findAll(postRequestedFor(urlEqualTo(sub.path())))).hasSize(1));
		assertThat(outboxRetryCount(sub.path())).isEqualTo(1);
	}

	@Test
	void noOutboxRowsAreWrittenForInactiveSubscriptions() {
		Subscription sub = subscribe("/hooks/inactive-" + UUID.randomUUID());
		api.delete().uri("/api/v1/webhooks/{id}", sub.id()).retrieve().toBodilessEntity();

		AccountView account = openAccount("USD");
		assertThat(deposit(account.accountNumber(), money("1.0000")).getStatusCode()).isEqualTo(HttpStatus.CREATED);
		assertThat(jdbc.sql("select count(*) from webhook_outbox where target_url = ?").param(WIREMOCK.baseUrl() + sub.path())
				.query(Long.class).single()).isZero();
	}

}
