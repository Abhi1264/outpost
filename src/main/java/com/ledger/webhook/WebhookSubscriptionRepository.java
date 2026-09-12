package com.ledger.webhook;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface WebhookSubscriptionRepository extends JpaRepository<WebhookSubscription, UUID> {

	List<WebhookSubscription> findByActiveTrue();

	List<WebhookSubscription> findByClientIdOrderByCreatedAtDesc(String clientId);

	Optional<WebhookSubscription> findByIdAndClientId(UUID id, String clientId);

}
