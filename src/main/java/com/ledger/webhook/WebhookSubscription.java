package com.ledger.webhook;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "webhook_subscriptions")
public class WebhookSubscription {

	@Id
	@GeneratedValue
	private UUID id;

	@Column(nullable = false, updatable = false, length = 128)
	private String clientId;

	@Column(nullable = false, length = 2048)
	private String targetUrl;

	@Column(nullable = false, updatable = false, length = 128)
	private String secretKey;

	@Column(name = "is_active", nullable = false)
	private boolean active;

	@Column(nullable = false, updatable = false)
	private Instant createdAt;

	protected WebhookSubscription() {
	}

	WebhookSubscription(String clientId, String targetUrl, String secretKey) {
		this.clientId = clientId;
		this.targetUrl = targetUrl;
		this.secretKey = secretKey;
		this.active = true;
		this.createdAt = Instant.now();
	}

	void deactivate() {
		this.active = false;
	}

	public UUID getId() {
		return id;
	}

	public String getClientId() {
		return clientId;
	}

	public String getTargetUrl() {
		return targetUrl;
	}

	public String getSecretKey() {
		return secretKey;
	}

	public boolean isActive() {
		return active;
	}

	public Instant getCreatedAt() {
		return createdAt;
	}

}
