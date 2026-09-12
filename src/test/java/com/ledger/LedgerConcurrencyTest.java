package com.ledger;

import com.ledger.ledger.LedgerApi.AccountView;
import com.ledger.ledger.LedgerApi.TransactionView;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class LedgerConcurrencyTest extends IntegrationTestBase {

	private static <T> List<T> runConcurrently(int n, IntFunction<T> call) {
		try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
			List<CompletableFuture<T>> futures = IntStream.range(0, n)
					.mapToObj(i -> CompletableFuture.supplyAsync(() -> call.apply(i), pool))
					.toList();
			return futures.stream().map(CompletableFuture::join).toList();
		}
	}

	@Test
	void concurrentDepositsAndWithdrawalsKeepBalanceAndJournalConsistent() {
		AccountView account = openAccount("USD");
		List<BigDecimal> deposits = IntStream.range(0, 100).mapToObj(i -> randomAmount()).toList();
		BigDecimal expected = deposits.stream().reduce(BigDecimal.ZERO, BigDecimal::add);

		List<ResponseEntity<TransactionView>> responses = runConcurrently(deposits.size(),
				i -> deposit(account.accountNumber(), deposits.get(i)));

		assertThat(responses).allSatisfy(r -> assertThat(r.getStatusCode()).isEqualTo(HttpStatus.CREATED));
		assertThat(dbBalance(account.accountNumber())).isEqualByComparingTo(expected);
		assertThat(ledgerNet(account.accountNumber())).isEqualByComparingTo(expected);
		assertThat(getAccount(account.accountNumber()).balance()).isEqualByComparingTo(expected);

		assertThat(clearingBalance("USD")).isEqualByComparingTo(clearingLedgerNet("USD"));
		assertThat(unbalancedTransactions()).isZero();
		assertThat(currencyTotal("USD")).isEqualByComparingTo(BigDecimal.ZERO);

		assertThat(responses).allSatisfy(r -> {
			TransactionView tx = r.getBody();
			assertThat(tx.entries()).hasSize(2);
			assertThat(tx.entries()).extracting(e -> e.entryType().name()).containsExactlyInAnyOrder("DEBIT", "CREDIT");
			assertThat(tx.entries()).allSatisfy(e -> assertThat(e.amount()).isEqualByComparingTo(tx.amount()));
		});
	}

	@Test
	void concurrentOverdraftAttemptsNeverPushBalanceBelowZero() {
		AccountView account = openAccount("EUR");
		assertThat(deposit(account.accountNumber(), money("100.0000")).getStatusCode()).isEqualTo(HttpStatus.CREATED);

		List<ResponseEntity<TransactionView>> responses = runConcurrently(25,
				i -> withdraw(account.accountNumber(), money("10.0000")));

		long succeeded = responses.stream().filter(r -> r.getStatusCode() == HttpStatus.CREATED).count();
		long rejected = responses.stream().filter(r -> r.getStatusCode() == HttpStatus.UNPROCESSABLE_CONTENT).count();
		assertThat(succeeded).isEqualTo(10);
		assertThat(rejected).isEqualTo(15);
		assertThat(dbBalance(account.accountNumber())).isEqualByComparingTo(BigDecimal.ZERO);
		assertThat(ledgerNet(account.accountNumber())).isEqualByComparingTo(BigDecimal.ZERO);
		assertThat(unbalancedTransactions()).isZero();
	}

	@Test
	void concurrentBidirectionalTransfersConserveMoneyAndDoNotDeadlock() {
		AccountView a = openAccount("GBP");
		AccountView b = openAccount("GBP");
		deposit(a.accountNumber(), money("1000.0000"));
		deposit(b.accountNumber(), money("1000.0000"));

		List<ResponseEntity<TransactionView>> responses = runConcurrently(100, i -> {
			boolean aToB = i % 2 == 0;
			return aToB
					? transfer(a.accountNumber(), b.accountNumber(), money("1.2500"))
					: transfer(b.accountNumber(), a.accountNumber(), money("1.2500"));
		});

		assertThat(responses).allSatisfy(r -> assertThat(r.getStatusCode()).isEqualTo(HttpStatus.CREATED));
		BigDecimal balanceA = dbBalance(a.accountNumber());
		BigDecimal balanceB = dbBalance(b.accountNumber());
		assertThat(balanceA.add(balanceB)).isEqualByComparingTo(money("2000.0000"));
		assertThat(balanceA).isEqualByComparingTo(ledgerNet(a.accountNumber()));
		assertThat(balanceB).isEqualByComparingTo(ledgerNet(b.accountNumber()));
		assertThat(unbalancedTransactions()).isZero();
		assertThat(currencyTotal("GBP")).isEqualByComparingTo(BigDecimal.ZERO);
	}

	@Test
	void transferBetweenDifferentCurrenciesIsRejected() {
		AccountView usd = openAccount("USD");
		AccountView jpy = openAccount("JPY");
		deposit(usd.accountNumber(), money("5.0000"));

		assertThat(transfer(usd.accountNumber(), jpy.accountNumber(), money("1.0000")).getStatusCode())
				.isEqualTo(HttpStatus.UNPROCESSABLE_CONTENT);
		assertThat(dbBalance(usd.accountNumber())).isEqualByComparingTo(money("5.0000"));
		assertThat(dbBalance(jpy.accountNumber())).isEqualByComparingTo(BigDecimal.ZERO);
	}

}
