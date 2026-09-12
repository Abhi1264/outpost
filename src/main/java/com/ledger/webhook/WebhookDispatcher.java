package com.ledger.webhook;

import com.fasterxml.jackson.annotation.JsonRawValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Consumes outbox ids from the dispatch topic and delivers them over HTTP. The claim
 * (PENDING -> PROCESSING) is atomic, the HTTP call runs outside any DB transaction, and the outcome
 * is recorded in its own short transaction.
 */
@Component
class WebhookDispatcher {

	private static final Logger log = LoggerFactory.getLogger(WebhookDispatcher.class);

	record DeadLetter(UUID outboxId, String eventType, String targetUrl, int attempts, String lastError,
			@JsonRawValue String payload) {
	}

	private final WebhookOutboxRepository outbox;
	private final RestClient http;
	private final KafkaTemplate<String, String> kafka;
	private final JsonMapper json;
	private final WebhookProperties properties;

	WebhookDispatcher(WebhookOutboxRepository outbox, RestClient.Builder httpBuilder, KafkaTemplate<String, String> kafka,
			JsonMapper json, WebhookProperties properties) {
		this.outbox = outbox;
		this.http = httpBuilder.build();
		this.kafka = kafka;
		this.json = json;
		this.properties = properties;
	}

	@KafkaListener(topics = "${outpost.webhook.dispatch-topic}")
	void onDispatch(String outboxId) {
		UUID id = UUID.fromString(outboxId);
		if (outbox.claim(id, Instant.now()) == 0) {
			return; // duplicate publish: not due, already claimed, or already finished
		}
		deliver(outbox.findById(id).orElseThrow());
	}

	private void deliver(WebhookOutbox row) {
		int attempt = row.getRetryCount() + 1;
		try {
			http.post()
					.uri(row.getTargetUrl())
					.contentType(MediaType.APPLICATION_JSON)
					.header("X-Signature-256", HmacSigner.sign(row.getPayload(), row.getSecretKey()))
					.header("X-Webhook-Id", row.getId().toString())
					.header("X-Webhook-Event", row.getEventType())
					.header("X-Webhook-Attempt", Integer.toString(attempt))
					.body(row.getPayload())
					.retrieve()
					.toBodilessEntity();
			outbox.markDelivered(row.getId(), Instant.now());
			log.info("Delivered webhook {} to {} on attempt {}", row.getId(), row.getTargetUrl(), attempt);
		}
		catch (RestClientResponseException ex) {
			HttpStatusCode status = ex.getStatusCode();
			fail(row, attempt, status.value() + " " + ex.getStatusText(), isPermanent(status));
		}
		catch (RestClientException ex) {
			fail(row, attempt, ex.getClass().getSimpleName() + ": " + ex.getMessage(), false);
		}
		catch (IllegalArgumentException ex) {
			fail(row, attempt, "Invalid target URL: " + ex.getMessage(), true);
		}
	}

	/** A 4xx is the endpoint rejecting the request itself; only 408 and 429 are transient. */
	private static boolean isPermanent(HttpStatusCode status) {
		return status.is4xxClientError() && status.value() != 408 && status.value() != 429;
	}

	private void fail(WebhookOutbox row, int attempt, String error, boolean permanent) {
		Instant now = Instant.now();
		if (permanent || attempt > properties.backoff().size()) {
			outbox.markFailed(row.getId(), attempt, error, now);
			log.error("Webhook {} to {} failed permanently after {} attempt(s): {}", row.getId(), row.getTargetUrl(),
					attempt, error);
			deadLetter(row, attempt, error);
			return;
		}
		Instant nextRetryAt = now.plus(withJitter(properties.backoff().get(attempt - 1)));
		outbox.scheduleRetry(row.getId(), attempt, nextRetryAt, error, now);
		log.warn("Webhook {} to {} failed attempt {} ({}); retry at {}", row.getId(), row.getTargetUrl(), attempt, error,
				nextRetryAt);
	}

	private Duration withJitter(Duration base) {
		if (properties.jitter() == 0) {
			return base;
		}
		double factor = 1 + ThreadLocalRandom.current().nextDouble(-properties.jitter(), properties.jitter());
		return Duration.ofMillis(Math.round(base.toMillis() * factor));
	}

	private void deadLetter(WebhookOutbox row, int attempts, String error) {
		String body = json.writeValueAsString(new DeadLetter(row.getId(), row.getEventType(), row.getTargetUrl(), attempts,
				error, row.getPayload()));
		kafka.send(properties.dlqTopic(), row.getId().toString(), body).whenComplete((result, ex) -> {
			if (ex != null) {
				log.error("Failed to publish outbox {} to DLQ {}", row.getId(), properties.dlqTopic(), ex);
			}
		});
	}

}
