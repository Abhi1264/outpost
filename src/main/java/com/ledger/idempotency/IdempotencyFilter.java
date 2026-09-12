package com.ledger.idempotency;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.ContentCachingResponseWrapper;
import tools.jackson.databind.json.JsonMapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.Optional;

/**
 * Idempotency for authenticated POSTs carrying an Idempotency-Key header, scoped per principal.
 * A 2xx response is stored and replayed for the same key; the same key with a different body is a 422;
 * a concurrent request for a key still in flight is a 409 (Redis SET NX). Failures are not stored, so
 * the key can be retried.
 */
@Component
public class IdempotencyFilter extends OncePerRequestFilter {

	public static final String HEADER = "Idempotency-Key";
	private static final int MAX_KEY_LENGTH = 128;
	private static final Duration LOCK_TTL = Duration.ofSeconds(30);
	private static final Logger log = LoggerFactory.getLogger(IdempotencyFilter.class);

	private record Stored(String requestHash, int statusCode, String responsePayload) {
	}

	private final JdbcClient jdbc;
	private final StringRedisTemplate redis;
	private final JsonMapper json;
	private final Duration ttl;

	IdempotencyFilter(JdbcClient jdbc, StringRedisTemplate redis, JsonMapper json,
			@Value("${outpost.idempotency.ttl}") Duration ttl) {
		this.jdbc = jdbc;
		this.redis = redis;
		this.json = json;
		this.ttl = ttl;
	}

	@Override
	protected boolean shouldNotFilter(HttpServletRequest request) {
		return !"POST".equals(request.getMethod()) || !StringUtils.hasText(request.getHeader(HEADER))
				|| request.getUserPrincipal() == null;
	}

	@Override
	protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
			throws ServletException, IOException {
		String header = request.getHeader(HEADER);
		if (header.length() > MAX_KEY_LENGTH) {
			writeProblem(response, HttpStatus.BAD_REQUEST, HEADER + " must be at most " + MAX_KEY_LENGTH + " characters");
			return;
		}
		String key = request.getUserPrincipal().getName() + ":" + header;
		byte[] body = request.getInputStream().readAllBytes();
		String requestHash = sha256(request.getMethod(), request.getRequestURI(), body);

		Optional<Stored> stored = find(key);
		if (stored.isPresent()) {
			if (!stored.get().requestHash().equals(requestHash)) {
				writeProblem(response, HttpStatus.UNPROCESSABLE_CONTENT,
						HEADER + " was already used with a different request");
				return;
			}
			replay(response, stored.get());
			return;
		}

		String lockKey = "idempotency:lock:" + key;
		if (!Boolean.TRUE.equals(redis.opsForValue().setIfAbsent(lockKey, "1", LOCK_TTL))) {
			writeProblem(response, HttpStatus.CONFLICT, "A request with this " + HEADER + " is already in progress");
			return;
		}
		ContentCachingResponseWrapper wrapped = new ContentCachingResponseWrapper(response);
		try {
			chain.doFilter(new CachedBodyRequest(request, body), wrapped);
			if (HttpStatusCode.valueOf(wrapped.getStatus()).is2xxSuccessful()) {
				store(key, requestHash, wrapped.getStatus(), wrapped.getContentAsByteArray());
			}
		}
		finally {
			redis.delete(lockKey);
			wrapped.copyBodyToResponse();
		}
	}

	private Optional<Stored> find(String key) {
		return jdbc.sql("select request_hash, status_code, response_payload::text from idempotency_keys where key = ?")
				.param(key)
				.query((rs, i) -> new Stored(rs.getString("request_hash"), rs.getInt("status_code"),
						rs.getString("response_payload")))
				.optional();
	}

	private void store(String key, String requestHash, int status, byte[] responseBody) {
		String payload = responseBody.length == 0 ? null : new String(responseBody, StandardCharsets.UTF_8);
		try {
			jdbc.sql("""
					insert into idempotency_keys (key, request_hash, status_code, response_payload, created_at)
					values (?, ?, ?, ?::jsonb, ?) on conflict (key) do nothing
					""")
					.params(key, requestHash, status, payload, Instant.now().atOffset(ZoneOffset.UTC))
					.update();
		}
		catch (DataAccessException ex) {
			// the ledger transaction is already committed; failing here would turn a success into a 500
			log.error("Failed to persist idempotency key {}", key, ex);
		}
	}

	private void replay(HttpServletResponse response, Stored stored) throws IOException {
		response.setStatus(stored.statusCode());
		response.setHeader("Idempotent-Replayed", "true");
		if (stored.responsePayload() != null) {
			response.setContentType(MediaType.APPLICATION_JSON_VALUE);
			response.getOutputStream().write(stored.responsePayload().getBytes(StandardCharsets.UTF_8));
		}
	}

	private void writeProblem(HttpServletResponse response, HttpStatus status, String detail) throws IOException {
		response.setStatus(status.value());
		response.setContentType(MediaType.APPLICATION_PROBLEM_JSON_VALUE);
		json.writeValue(response.getOutputStream(), ProblemDetail.forStatusAndDetail(status, detail));
	}

	@Scheduled(cron = "0 0 * * * *")
	void purgeExpired() {
		int deleted = jdbc.sql("delete from idempotency_keys where created_at < ?")
				.param(Instant.now().minus(ttl).atOffset(ZoneOffset.UTC))
				.update();
		if (deleted > 0) {
			log.info("Purged {} expired idempotency keys", deleted);
		}
	}

	private static String sha256(String method, String uri, byte[] body) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			digest.update(method.getBytes(StandardCharsets.UTF_8));
			digest.update((byte) '\n');
			digest.update(uri.getBytes(StandardCharsets.UTF_8));
			digest.update((byte) '\n');
			digest.update(body);
			return HexFormat.of().formatHex(digest.digest());
		}
		catch (NoSuchAlgorithmException ex) {
			throw new IllegalStateException("SHA-256 unavailable", ex);
		}
	}

	private static final class CachedBodyRequest extends HttpServletRequestWrapper {

		private final byte[] body;

		CachedBodyRequest(HttpServletRequest request, byte[] body) {
			super(request);
			this.body = body;
		}

		@Override
		public ServletInputStream getInputStream() {
			ByteArrayInputStream delegate = new ByteArrayInputStream(body);
			return new ServletInputStream() {
				@Override
				public int read() throws IOException {
					return delegate.read();
				}

				@Override
				public int read(byte[] b, int off, int len) throws IOException {
					return delegate.read(b, off, len);
				}

				@Override
				public boolean isFinished() {
					return delegate.available() == 0;
				}

				@Override
				public boolean isReady() {
					return true;
				}

				@Override
				public void setReadListener(ReadListener listener) {
					throw new UnsupportedOperationException();
				}
			};
		}

	}

}
