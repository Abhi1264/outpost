package com.ledger;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.ledger.idempotency.IdempotencyFilter;
import com.ledger.ledger.LedgerApi.AccountView;
import com.ledger.ledger.LedgerApi.TransactionView;
import net.datafaker.Faker;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.client.RestClient;

import java.math.BigDecimal;
import java.util.Map;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;

@Import(TestcontainersConfiguration.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
		"spring.security.user.name=" + IntegrationTestBase.USER,
		"spring.security.user.password=" + IntegrationTestBase.PASSWORD,
		"outpost.webhook.backoff=200ms,200ms,200ms",
		"outpost.webhook.jitter=0",
		"outpost.webhook.poll-interval=PT0.2S",
		"outpost.webhook.poll-grace=0s",
		"outpost.webhook.stale-processing-after=30s" })
abstract class IntegrationTestBase {

	static final String USER = "test-client";
	static final String PASSWORD = "test-secret";
	static final Faker FAKER = new Faker();

	static final WireMockServer WIREMOCK = new WireMockServer(options().dynamicPort());
	static {
		WIREMOCK.start();
	}

	@LocalServerPort
	int port;

	@Autowired
	JdbcClient jdbc;

	@Autowired
	RestClient.Builder restClientBuilder;

	RestClient api;

	@BeforeEach
	void buildClient() {
		api = restClientBuilder.clone()
				.baseUrl("http://localhost:" + port)
				.defaultHeaders(h -> h.setBasicAuth(USER, PASSWORD))
				.defaultStatusHandler(HttpStatusCode::isError, (request, response) -> {
				})
				.build();
	}

	AccountView openAccount(String currency) {
		return api.post().uri("/api/v1/accounts").contentType(MediaType.APPLICATION_JSON)
				.body(Map.of("currency", currency)).retrieve().body(AccountView.class);
	}

	AccountView getAccount(String accountNumber) {
		return api.get().uri("/api/v1/accounts/{n}", accountNumber).retrieve().body(AccountView.class);
	}

	ResponseEntity<TransactionView> deposit(String accountNumber, BigDecimal amount) {
		return amountPost("/api/v1/accounts/{n}/deposits", accountNumber, amount, null);
	}

	ResponseEntity<TransactionView> deposit(String accountNumber, BigDecimal amount, String idempotencyKey) {
		return amountPost("/api/v1/accounts/{n}/deposits", accountNumber, amount, idempotencyKey);
	}

	ResponseEntity<TransactionView> withdraw(String accountNumber, BigDecimal amount) {
		return amountPost("/api/v1/accounts/{n}/withdrawals", accountNumber, amount, null);
	}

	ResponseEntity<TransactionView> transfer(String from, String to, BigDecimal amount) {
		return api.post().uri("/api/v1/transfers").contentType(MediaType.APPLICATION_JSON)
				.body(Map.of("fromAccountNumber", from, "toAccountNumber", to, "amount", amount))
				.retrieve().toEntity(TransactionView.class);
	}

	private ResponseEntity<TransactionView> amountPost(String uri, String accountNumber, BigDecimal amount, String key) {
		return api.post().uri(uri, accountNumber).contentType(MediaType.APPLICATION_JSON)
				.headers(h -> {
					if (key != null) {
						h.set(IdempotencyFilter.HEADER, key);
					}
				})
				.body(Map.of("amount", amount))
				.retrieve().toEntity(TransactionView.class);
	}

	BigDecimal money(String value) {
		return new BigDecimal(value).setScale(4);
	}

	BigDecimal randomAmount() {
		return BigDecimal.valueOf(FAKER.number().numberBetween(1L, 9_999_999L), 4);
	}

	BigDecimal dbBalance(String accountNumber) {
		return jdbc.sql("select balance from accounts where account_number = ?").param(accountNumber)
				.query(BigDecimal.class).single();
	}

	/** Credits minus debits; must always equal the stored balance. */
	BigDecimal ledgerNet(String accountNumber) {
		return ledgerNetWhereAccountNumber("= ?", accountNumber);
	}

	BigDecimal clearingBalance(String currency) {
		return jdbc.sql("select coalesce(sum(balance), 0) from accounts where kind = 'SYSTEM' and currency = ?")
				.param(currency).query(BigDecimal.class).single();
	}

	BigDecimal clearingLedgerNet(String currency) {
		return ledgerNetWhereAccountNumber("like ?", "SYS-CLEARING-" + currency + "-%");
	}

	private BigDecimal ledgerNetWhereAccountNumber(String operator, String value) {
		return jdbc.sql("""
				select coalesce(sum(case when e.entry_type = 'CREDIT' then e.amount else -e.amount end), 0)
				from ledger_entries e join accounts a on a.id = e.account_id where a.account_number %s
				""".formatted(operator)).param(value).query(BigDecimal.class).single();
	}

	/** Transactions that are not exactly one DEBIT + one CREDIT of equal amount. */
	long unbalancedTransactions() {
		return jdbc.sql("""
				select count(*) from (
				  select transaction_id from ledger_entries group by transaction_id
				  having count(*) <> 2
				      or sum(case when entry_type = 'DEBIT' then amount else -amount end) <> 0
				) unbalanced
				""").query(Long.class).single();
	}

	/** Customer + clearing balances per currency always sum to zero. */
	BigDecimal currencyTotal(String currency) {
		return jdbc.sql("select coalesce(sum(balance), 0) from accounts where currency = ?").param(currency)
				.query(BigDecimal.class).single();
	}

}
