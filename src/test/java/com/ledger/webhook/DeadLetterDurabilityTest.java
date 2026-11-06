package com.ledger.webhook;

import com.ledger.IntegrationTestBase;
import com.ledger.ledger.LedgerApi.AccountView;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.JsonNode;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DeadLetterDurabilityTest extends IntegrationTestBase {

	@Autowired
	WebhookDispatcher dispatcher;

	@Test
	void permanentFailureKeepsTheDeadLetterRowWhenKafkaSendThrows() {
		@SuppressWarnings("unchecked")
		KafkaTemplate<String, String> failing = mock(KafkaTemplate.class);
		when(failing.send(anyString(), anyString(), anyString())).thenThrow(new RuntimeException("down"));
		KafkaTemplate<String, String> real = dispatcher.kafka;
		dispatcher.kafka = failing;
		String path = "/hooks/dlq-down-" + UUID.randomUUID();
		UUID subscriptionId = null;
		try {
			JsonNode created = api.post().uri("/api/v1/webhooks").contentType(MediaType.APPLICATION_JSON)
					.body(Map.of("targetUrl", WIREMOCK.baseUrl() + path)).retrieve().body(JsonNode.class);
			subscriptionId = UUID.fromString(created.get("id").asString());
			WIREMOCK.stubFor(post(urlEqualTo(path)).willReturn(aResponse().withStatus(410)));

			AccountView account = openAccount("USD");
			deposit(account.accountNumber(), money("1.0000"));

			verify(failing, timeout(15_000)).send(anyString(), anyString(), anyString());
			String url = WIREMOCK.baseUrl() + path;
			await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(
					jdbc.sql("select status from webhook_outbox where target_url = ?").param(url).query(String.class).single())
					.isEqualTo("FAILED"));
			assertThat(jdbc.sql("select payload::text from webhook_outbox where target_url = ?").param(url)
					.query(String.class).single()).contains("transaction.completed");
		}
		finally {
			dispatcher.kafka = real;
			if (subscriptionId != null) {
				api.delete().uri("/api/v1/webhooks/{id}", subscriptionId).retrieve().toBodilessEntity();
			}
			WIREMOCK.resetAll();
		}
	}

}
