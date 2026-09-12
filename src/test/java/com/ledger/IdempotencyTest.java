package com.ledger;

import com.ledger.idempotency.IdempotencyFilter;
import com.ledger.ledger.LedgerApi.AccountView;
import com.ledger.ledger.LedgerApi.TransactionView;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class IdempotencyTest extends IntegrationTestBase {

	@Test
	void sameKeyReplaysTheOriginalResponseWithoutRepostingTheLedger() {
		AccountView account = openAccount("USD");
		String key = UUID.randomUUID().toString();

		ResponseEntity<TransactionView> first = deposit(account.accountNumber(), money("42.0000"), key);
		ResponseEntity<TransactionView> second = deposit(account.accountNumber(), money("42.0000"), key);

		assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
		assertThat(second.getStatusCode()).isEqualTo(HttpStatus.CREATED);
		assertThat(second.getHeaders().getFirst("Idempotent-Replayed")).isEqualTo("true");
		assertThat(second.getBody()).isEqualTo(first.getBody());
		assertThat(dbBalance(account.accountNumber())).isEqualByComparingTo(money("42.0000"));
		assertThat(jdbc.sql("select count(*) from ledger_entries where transaction_id = ?")
				.param(first.getBody().transactionId()).query(Long.class).single()).isEqualTo(2L);
	}

	@Test
	void sameKeyWithDifferentBodyIsRejected() {
		AccountView account = openAccount("USD");
		String key = UUID.randomUUID().toString();

		assertThat(deposit(account.accountNumber(), money("1.0000"), key).getStatusCode()).isEqualTo(HttpStatus.CREATED);
		ResponseEntity<String> mismatch = api.post().uri("/api/v1/accounts/{n}/deposits", account.accountNumber())
				.contentType(MediaType.APPLICATION_JSON)
				.header(IdempotencyFilter.HEADER, key)
				.body(Map.of("amount", money("2.0000")))
				.retrieve().toEntity(String.class);

		assertThat(mismatch.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_CONTENT);
		assertThat(mismatch.getHeaders().getContentType()).isEqualTo(MediaType.APPLICATION_PROBLEM_JSON);
		assertThat(mismatch.getBody()).contains("different request");
		assertThat(dbBalance(account.accountNumber())).isEqualByComparingTo(money("1.0000"));
	}

	@Test
	void failedRequestsAreNotCachedSoTheKeyCanBeRetried() {
		AccountView account = openAccount("USD");
		String key = UUID.randomUUID().toString();

		ResponseEntity<String> overdraft = api.post().uri("/api/v1/accounts/{n}/withdrawals", account.accountNumber())
				.contentType(MediaType.APPLICATION_JSON).header(IdempotencyFilter.HEADER, key)
				.body(Map.of("amount", money("5.0000"))).retrieve().toEntity(String.class);
		assertThat(overdraft.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_CONTENT);

		deposit(account.accountNumber(), money("5.0000"));
		ResponseEntity<String> retry = api.post().uri("/api/v1/accounts/{n}/withdrawals", account.accountNumber())
				.contentType(MediaType.APPLICATION_JSON).header(IdempotencyFilter.HEADER, key)
				.body(Map.of("amount", money("5.0000"))).retrieve().toEntity(String.class);
		assertThat(retry.getStatusCode()).isEqualTo(HttpStatus.CREATED);
		assertThat(dbBalance(account.accountNumber())).isEqualByComparingTo(BigDecimal.ZERO);
	}

	@Test
	void concurrentRequestsWithSameKeyExecuteExactlyOnce() {
		AccountView account = openAccount("USD");
		String key = UUID.randomUUID().toString();

		List<HttpStatusCode> statuses;
		try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
			statuses = IntStream.range(0, 20)
					.mapToObj(i -> CompletableFuture.supplyAsync(
							() -> deposit(account.accountNumber(), money("7.0000"), key).getStatusCode(), pool))
					.toList().stream().map(CompletableFuture::join).toList();
		}

		assertThat(statuses).allSatisfy(s -> assertThat(s).isIn(HttpStatus.CREATED, HttpStatus.CONFLICT));
		assertThat(statuses).contains(HttpStatus.CREATED);
		assertThat(dbBalance(account.accountNumber())).isEqualByComparingTo(money("7.0000"));
	}

	@Test
	void keysAreScopedPerPrincipalAndRequireAuthentication() {
		AccountView account = openAccount("USD");
		ResponseEntity<String> anonymous = restClientBuilder.clone().baseUrl("http://localhost:" + port)
				.defaultStatusHandler(HttpStatusCode::isError, (request, response) -> {
				})
				.build()
				.post().uri("/api/v1/accounts/{n}/deposits", account.accountNumber())
				.contentType(MediaType.APPLICATION_JSON).header(IdempotencyFilter.HEADER, "anon")
				.body(Map.of("amount", money("1.0000"))).retrieve().toEntity(String.class);

		assertThat(anonymous.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
		assertThat(jdbc.sql("select count(*) from idempotency_keys where key like '%:anon'").query(Long.class).single())
				.isZero();
	}

}
