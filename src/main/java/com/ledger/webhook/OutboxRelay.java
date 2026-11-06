package com.ledger.webhook;

import com.ledger.ledger.TransactionCommittedEvent;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Publishes outbox row ids to the Kafka dispatch topic: immediately AFTER_COMMIT, and from a poller
 * for retries, failed publishes and rows orphaned by a crashed worker. Duplicate publishes are
 * harmless because WebhookDispatcher claims rows atomically.
 */
@Component
class OutboxRelay {

	private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
	private static final List<WebhookOutbox.Status> IN_FLIGHT =
			List.of(WebhookOutbox.Status.PENDING, WebhookOutbox.Status.PROCESSING);

	private final WebhookOutboxRepository outbox;
	private final KafkaTemplate<String, String> kafka;
	private final WebhookProperties properties;

	OutboxRelay(WebhookOutboxRepository outbox, KafkaTemplate<String, String> kafka, WebhookProperties properties,
			MeterRegistry meters) {
		this.outbox = outbox;
		this.kafka = kafka;
		this.properties = properties;
		Gauge.builder("outpost.webhook.outbox.inflight", outbox, o -> (double) o.countByStatusIn(IN_FLIGHT))
				.register(meters);
	}

	@TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
	void onTransactionCommitted(TransactionCommittedEvent event) {
		event.outboxIds().forEach(this::publish);
	}

	@Scheduled(fixedDelayString = "${outpost.webhook.poll-interval}")
	void relayDue() {
		Instant now = Instant.now();
		int reclaimed = outbox.reclaimStale(now.minus(properties.staleProcessingAfter()), now);
		if (reclaimed > 0) {
			log.warn("Returned {} stale PROCESSING outbox rows to PENDING", reclaimed);
		}
		outbox.findDueIds(now, Limit.of(properties.pollBatchSize())).forEach(this::publish);
	}

	private void publish(UUID outboxId) {
		String id = outboxId.toString();
		kafka.send(properties.dispatchTopic(), id, id).whenComplete((result, ex) -> {
			if (ex != null) {
				log.warn("Failed to publish outbox {} to {}; poller will retry", id, properties.dispatchTopic(), ex);
			}
		});
	}

}
