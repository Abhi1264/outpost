package com.ledger.webhook;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;
import java.util.List;

@Validated
@ConfigurationProperties("outpost.webhook")
public record WebhookProperties(
		@NotEmpty List<Duration> backoff,
		@DecimalMin("0.0") @DecimalMax("1.0") double jitter,
		@NotNull Duration pollInterval,
		@Positive int pollBatchSize,
		@NotNull Duration staleProcessingAfter,
		@NotBlank String dispatchTopic,
		@NotBlank String dlqTopic) {
}
