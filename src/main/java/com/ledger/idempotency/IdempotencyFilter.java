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
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.UnexpectedRollbackException;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.StringUtils;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.ContentCachingResponseWrapper;
import tools.jackson.databind.json.JsonMapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
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
 * The key row is inserted in the same database transaction as the ledger write. A 2xx is stored and
 * replayed; the same key with a different body is a 422. Anything else rolls the key back.
 */
@Component
public class IdempotencyFilter extends OncePerRequestFilter {

	public static final String HEADER = "Idempotency-Key";
	private static final int MAX_KEY_LENGTH = 128;
	private static final int IN_FLIGHT = 0;
	private static final Logger log = LoggerFactory.getLogger(IdempotencyFilter.class);

	private record Stored(String requestHash, int statusCode, String responsePayload) {
	}

	private final JdbcClient jdbc;
	private final JsonMapper json;
	private final TransactionTemplate transactions;
	private final Duration ttl;

	IdempotencyFilter(JdbcClient jdbc, JsonMapper json, PlatformTransactionManager tx,
			@Value("${outpost.idempotency.ttl}") Duration ttl) {
		this.jdbc = jdbc;
		this.json = json;
		this.ttl = ttl;
		this.transactions = new TransactionTemplate(tx);
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
		ContentCachingResponseWrapper wrapped = new ContentCachingResponseWrapper(response);
		ServletException[] servletFailure = new ServletException[1];
		IOException[] ioFailure = new IOException[1];
		try {
			transactions.executeWithoutResult(status -> {
				if (!insert(key, requestHash)) {
					conflict(key, requestHash, wrapped);
					return;
				}
				try {
					chain.doFilter(new CachedBodyRequest(request, body), wrapped);
				}
				catch (IOException ex) {
					status.setRollbackOnly();
					ioFailure[0] = ex;
					return;
				}
				catch (ServletException ex) {
					status.setRollbackOnly();
					servletFailure[0] = ex;
					return;
				}
				// ControllerAdvice turns LedgerException into a 4xx and marks this transaction rollback-only.
				if (status.isRollbackOnly() || !is2xx(wrapped)) {
					status.setRollbackOnly();
					return;
				}
				store(key, wrapped.getStatus(), wrapped.getContentAsByteArray());
			});
			wrapped.copyBodyToResponse();
		}
		catch (UnexpectedRollbackException ex) {
			if (servletFailure[0] != null) {
				throw servletFailure[0];
			}
			if (ioFailure[0] != null) {
				throw ioFailure[0];
			}
			if (is2xx(wrapped)) {
				reset(response);
				throw ex;
			}
			wrapped.copyBodyToResponse();
		}
		catch (RuntimeException ex) {
			reset(response);
			throw ex;
		}
	}

	private boolean insert(String key, String requestHash) {
		return jdbc.sql("""
				insert into idempotency_keys (key, request_hash, status_code)
				values (?, ?, 0) on conflict (key) do nothing
				""").params(key, requestHash).update() == 1;
	}

	private void conflict(String key, String requestHash, HttpServletResponse response) {
		try {
			Stored row = find(key).orElse(null);
			if (row == null || row.statusCode() == IN_FLIGHT) {
				writeProblem(response, HttpStatus.CONFLICT, "A request with this " + HEADER + " is already in progress");
				return;
			}
			if (!row.requestHash().equals(requestHash)) {
				writeProblem(response, HttpStatus.UNPROCESSABLE_CONTENT,
						HEADER + " was already used with a different request");
				return;
			}
			replay(response, row);
		}
		catch (IOException ex) {
			throw new UncheckedIOException(ex);
		}
	}

	private void store(String key, int status, byte[] responseBody) {
		String payload = responseBody.length == 0 ? null : new String(responseBody, StandardCharsets.UTF_8);
		int updated = jdbc.sql("""
				update idempotency_keys set status_code = ?, response_payload = ?::jsonb where key = ?
				""").params(status, payload, key).update();
		if (updated != 1) {
			throw new IllegalStateException("idempotency key was not stored");
		}
	}

	private Optional<Stored> find(String key) {
		return jdbc.sql("select request_hash, status_code, response_payload::text from idempotency_keys where key = ?")
				.param(key)
				.query((rs, i) -> new Stored(rs.getString("request_hash"), rs.getInt("status_code"),
						rs.getString("response_payload")))
				.optional();
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

	private static boolean is2xx(HttpServletResponse response) {
		return HttpStatusCode.valueOf(response.getStatus()).is2xxSuccessful();
	}

	private static void reset(HttpServletResponse response) {
		try {
			if (!response.isCommitted()) {
				response.reset();
			}
		}
		catch (IllegalStateException ignored) {
			// too late to clear a buffered status; the error still propagates
		}
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
