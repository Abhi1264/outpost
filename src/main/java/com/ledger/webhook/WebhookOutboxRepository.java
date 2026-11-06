package com.ledger.webhook;

import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

public interface WebhookOutboxRepository extends JpaRepository<WebhookOutbox, UUID> {

	/** Returns 0 if the row is not due or another worker already owns it. */
	@Transactional
	@Modifying(clearAutomatically = true)
	@Query("""
			update WebhookOutbox o set o.status = com.ledger.webhook.WebhookOutbox.Status.PROCESSING, o.updatedAt = :now
			where o.id = :id and o.status = com.ledger.webhook.WebhookOutbox.Status.PENDING and o.nextRetryAt <= :now
			""")
	int claim(UUID id, Instant now);

	@Transactional
	@Modifying(clearAutomatically = true)
	@Query("""
			update WebhookOutbox o set o.status = com.ledger.webhook.WebhookOutbox.Status.DELIVERED, o.lastError = null, o.updatedAt = :now
			where o.id = :id
			""")
	void markDelivered(UUID id, Instant now);

	@Transactional
	@Modifying(clearAutomatically = true)
	@Query("""
			update WebhookOutbox o set o.status = com.ledger.webhook.WebhookOutbox.Status.PENDING, o.retryCount = :retryCount,
			o.nextRetryAt = :nextRetryAt, o.lastError = :error, o.updatedAt = :now
			where o.id = :id
			""")
	void scheduleRetry(UUID id, int retryCount, Instant nextRetryAt, String error, Instant now);

	@Transactional
	@Modifying(clearAutomatically = true)
	@Query("""
			update WebhookOutbox o set o.status = com.ledger.webhook.WebhookOutbox.Status.FAILED, o.retryCount = :retryCount,
			o.lastError = :error, o.updatedAt = :now
			where o.id = :id
			""")
	void markFailed(UUID id, int retryCount, String error, Instant now);

	@Query("""
			select o.id from WebhookOutbox o
			where o.status = com.ledger.webhook.WebhookOutbox.Status.PENDING and o.nextRetryAt <= :dueBefore
			order by o.nextRetryAt
			""")
	List<UUID> findDueIds(Instant dueBefore, Limit limit);

	long countByStatusIn(Collection<WebhookOutbox.Status> statuses);

	@Transactional
	@Modifying(clearAutomatically = true)
	@Query("""
			update WebhookOutbox o set o.status = com.ledger.webhook.WebhookOutbox.Status.PENDING, o.nextRetryAt = :now, o.updatedAt = :now
			where o.status = com.ledger.webhook.WebhookOutbox.Status.PROCESSING and o.updatedAt < :cutoff
			""")
	int reclaimStale(Instant cutoff, Instant now);

}
