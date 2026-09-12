package com.ledger.ledger;

import com.ledger.common.LedgerException;
import com.ledger.ledger.LedgerApi.AccountView;
import com.ledger.ledger.LedgerApi.EntryView;
import com.ledger.ledger.LedgerApi.TransactionType;
import com.ledger.ledger.LedgerApi.TransactionView;
import com.ledger.webhook.OutboxWriter;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Double-entry ledger. Every posting locks the involved account rows (SELECT ... FOR UPDATE) and writes
 * one DEBIT and one CREDIT entry plus the webhook outbox rows in a single transaction.
 *
 * Lock ordering (deadlock-free): customer accounts ascending by account number, then the system
 * clearing account. Money entering or leaving the ledger is booked against one of
 * {@value #CLEARING_SHARDS} clearing accounts per currency; a customer account always uses the same
 * shard, so shards never lock each other and hot-row contention is spread.
 */
@Service
public class LedgerService {

	public static final String TRANSACTION_COMPLETED = "transaction.completed";
	static final String CLEARING_PREFIX = "SYS-CLEARING-";
	static final int CLEARING_SHARDS = 8;

	private final AccountRepository accounts;
	private final LedgerEntryRepository entries;
	private final OutboxWriter outbox;
	private final ApplicationEventPublisher events;

	LedgerService(AccountRepository accounts, LedgerEntryRepository entries, OutboxWriter outbox,
			ApplicationEventPublisher events) {
		this.accounts = accounts;
		this.entries = entries;
		this.outbox = outbox;
		this.events = events;
	}

	@Transactional
	public AccountView openAccount(String currency) {
		return AccountView.from(accounts.save(new Account(newAccountNumber(), Account.Kind.CUSTOMER, currency)));
	}

	@Transactional(readOnly = true)
	public AccountView getAccount(String accountNumber) {
		return accounts.findByAccountNumber(accountNumber)
				.filter(a -> a.getKind() == Account.Kind.CUSTOMER)
				.map(AccountView::from)
				.orElseThrow(() -> LedgerException.accountNotFound(accountNumber));
	}

	@Transactional(readOnly = true)
	public List<EntryView> entries(String accountNumber, Pageable pageable) {
		return entries.findByAccountNumber(accountNumber, pageable).stream().map(EntryView::from).toList();
	}

	@Transactional
	public TransactionView deposit(String accountNumber, BigDecimal amount) {
		Account customer = lockCustomer(accountNumber);
		Account clearing = lockClearing(customer);
		return post(TransactionType.DEPOSIT, clearing, customer, Money.of(amount));
	}

	@Transactional
	public TransactionView withdraw(String accountNumber, BigDecimal amount) {
		Account customer = lockCustomer(accountNumber);
		Account clearing = lockClearing(customer);
		return post(TransactionType.WITHDRAWAL, customer, clearing, Money.of(amount));
	}

	@Transactional
	public TransactionView transfer(String fromAccountNumber, String toAccountNumber, BigDecimal amount) {
		if (fromAccountNumber.equals(toAccountNumber)) {
			throw LedgerException.invalid("Source and destination accounts must differ");
		}
		boolean fromFirst = fromAccountNumber.compareTo(toAccountNumber) < 0;
		Account first = lockCustomer(fromFirst ? fromAccountNumber : toAccountNumber);
		Account second = lockCustomer(fromFirst ? toAccountNumber : fromAccountNumber);
		Account source = fromFirst ? first : second;
		Account destination = fromFirst ? second : first;
		if (!source.getCurrency().equals(destination.getCurrency())) {
			throw LedgerException.currencyMismatch(source.getCurrency(), destination.getCurrency());
		}
		return post(TransactionType.TRANSFER, source, destination, Money.of(amount));
	}

	private TransactionView post(TransactionType type, Account debited, Account credited, BigDecimal amount) {
		UUID transactionId = UUID.randomUUID();
		Instant now = Instant.now();
		BigDecimal debitedBalance = debited.debit(amount);
		BigDecimal creditedBalance = credited.credit(amount);
		List<LedgerEntry> posted = entries.saveAll(List.of(
				new LedgerEntry(transactionId, debited, LedgerEntry.Type.DEBIT, amount, debitedBalance, now),
				new LedgerEntry(transactionId, credited, LedgerEntry.Type.CREDIT, amount, creditedBalance, now)));

		TransactionView view = new TransactionView(transactionId, type, debited.getCurrency(), amount, now,
				posted.stream().map(EntryView::from).toList());
		List<UUID> outboxIds = outbox.enqueue(TRANSACTION_COMPLETED, view);
		if (!outboxIds.isEmpty()) {
			events.publishEvent(new TransactionCommittedEvent(transactionId, outboxIds));
		}
		return view;
	}

	private Account lockCustomer(String accountNumber) {
		return accounts.lockByAccountNumber(accountNumber)
				.filter(a -> a.getKind() == Account.Kind.CUSTOMER)
				.orElseThrow(() -> LedgerException.accountNotFound(accountNumber));
	}

	/** Clearing shards are created on first use; a concurrent creator blocks on the unique index until we commit. */
	private Account lockClearing(Account customer) {
		String number = clearingAccountFor(customer.getAccountNumber(), customer.getCurrency());
		return accounts.lockByAccountNumber(number).orElseGet(() -> {
			accounts.insertSystemAccountIfAbsent(number, customer.getCurrency());
			return accounts.lockByAccountNumber(number).orElseThrow();
		});
	}

	static String clearingAccountFor(String customerAccountNumber, String currency) {
		return CLEARING_PREFIX + currency + "-" + Math.floorMod(customerAccountNumber.hashCode(), CLEARING_SHARDS);
	}

	private static String newAccountNumber() {
		return "ACC%012d".formatted(ThreadLocalRandom.current().nextLong(1_000_000_000_000L));
	}

}
