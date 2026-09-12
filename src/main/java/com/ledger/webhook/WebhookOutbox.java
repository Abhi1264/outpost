package com.ledger.webhook;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "webhook_outbox")
public class WebhookOutbox {

	public enum Status { PENDING, PROCESSING, DELIVERED, FAILED }

	@Id
	@GeneratedValue
	private UUID id;

	@Column(nullable = false, updatable = false, length = 64)
	private String eventType;

	@JdbcTypeCode(SqlTypes.JSON)
	@Column(nullable = false, updatable = false)
	private String payload;

	@Column(nullable = false, updatable = false, length = 2048)
	private String targetUrl;

	@Column(nullable = false, updatable = false, length = 128)
	private String secretKey;

	@Enumerated(EnumType.STRING)
	@Column(nullable = false, length = 16)
	private Status status;

	@Column(nullable = false)
	private int retryCount;

	@Column(nullable = false)
	private Instant nextRetryAt;

	private String lastError;

	@Column(nullable = false, updatable = false)
	private Instant createdAt;

	@Column(nullable = false)
	private Instant updatedAt;

	protected WebhookOutbox() {
	}

	WebhookOutbox(String eventType, String payload, String targetUrl, String secretKey) {
		this.eventType = eventType;
		this.payload = payload;
		this.targetUrl = targetUrl;
		this.secretKey = secretKey;
		this.status = Status.PENDING;
		this.retryCount = 0;
		this.createdAt = Instant.now();
		this.nextRetryAt = this.createdAt;
		this.updatedAt = this.createdAt;
	}

	public UUID getId() {
		return id;
	}

	public String getEventType() {
		return eventType;
	}

	public String getPayload() {
		return payload;
	}

	public String getTargetUrl() {
		return targetUrl;
	}

	public String getSecretKey() {
		return secretKey;
	}

	public Status getStatus() {
		return status;
	}

	public int getRetryCount() {
		return retryCount;
	}

	public Instant getNextRetryAt() {
		return nextRetryAt;
	}

	public String getLastError() {
		return lastError;
	}

}
