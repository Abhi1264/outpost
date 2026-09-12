package com.ledger.webhook;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Writes outbox rows inside the caller's transaction so the event commits with the ledger or not at all. */
@Component
public class OutboxWriter {

	record Envelope(String eventType, Instant occurredAt, Object data) {
	}

	private final WebhookSubscriptionRepository subscriptions;
	private final WebhookOutboxRepository outbox;
	private final JsonMapper json;

	OutboxWriter(WebhookSubscriptionRepository subscriptions, WebhookOutboxRepository outbox, JsonMapper json) {
		this.subscriptions = subscriptions;
		this.outbox = outbox;
		this.json = json;
	}

	@Transactional(propagation = Propagation.MANDATORY)
	public List<UUID> enqueue(String eventType, Object data) {
		List<WebhookSubscription> active = subscriptions.findByActiveTrue();
		if (active.isEmpty()) {
			return List.of();
		}
		String payload = json.writeValueAsString(new Envelope(eventType, Instant.now(), data));
		List<WebhookOutbox> rows = active.stream()
				.map(s -> new WebhookOutbox(eventType, payload, s.getTargetUrl(), s.getSecretKey()))
				.toList();
		return outbox.saveAll(rows).stream().map(WebhookOutbox::getId).toList();
	}

}
