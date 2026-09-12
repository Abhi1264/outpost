package com.ledger.webhook;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/webhooks")
class WebhookSubscriptionController {

	private static final SecureRandom RANDOM = new SecureRandom();

	private final WebhookSubscriptionRepository subscriptions;

	WebhookSubscriptionController(WebhookSubscriptionRepository subscriptions) {
		this.subscriptions = subscriptions;
	}

	record CreateSubscriptionRequest(
			@NotBlank @Size(max = 2048) @Pattern(regexp = "^https?://.+", message = "must be an http(s) URL") String targetUrl) {
	}

	@JsonInclude(JsonInclude.Include.NON_NULL)
	record SubscriptionView(UUID id, String clientId, String targetUrl, boolean active, Instant createdAt,
			String secret) {

		static SubscriptionView from(WebhookSubscription s, boolean includeSecret) {
			return new SubscriptionView(s.getId(), s.getClientId(), s.getTargetUrl(), s.isActive(), s.getCreatedAt(),
					includeSecret ? s.getSecretKey() : null);
		}
	}

	@PostMapping
	@ResponseStatus(HttpStatus.CREATED)
	SubscriptionView create(@Valid @RequestBody CreateSubscriptionRequest request, Authentication auth) {
		byte[] secret = new byte[32];
		RANDOM.nextBytes(secret);
		WebhookSubscription saved = subscriptions.save(new WebhookSubscription(auth.getName(), request.targetUrl(),
				Base64.getUrlEncoder().withoutPadding().encodeToString(secret)));
		return SubscriptionView.from(saved, true);
	}

	@GetMapping
	List<SubscriptionView> list(Authentication auth) {
		return subscriptions.findByClientIdOrderByCreatedAtDesc(auth.getName()).stream()
				.map(s -> SubscriptionView.from(s, false))
				.toList();
	}

	@DeleteMapping("/{id}")
	@ResponseStatus(HttpStatus.NO_CONTENT)
	@Transactional
	void deactivate(@PathVariable UUID id, Authentication auth) {
		subscriptions.findByIdAndClientId(id, auth.getName())
				.orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Subscription not found"))
				.deactivate();
	}

}
