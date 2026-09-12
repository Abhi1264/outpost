package com.ledger.ledger;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "ledger_entries")
public class LedgerEntry {

	/** DEBIT decreases the account balance, CREDIT increases it. */
	public enum Type { DEBIT, CREDIT }

	@Id
	@GeneratedValue
	private UUID id;

	@Column(nullable = false, updatable = false)
	private UUID transactionId;

	@ManyToOne(fetch = FetchType.LAZY, optional = false)
	@JoinColumn(name = "account_id", nullable = false, updatable = false)
	private Account account;

	@Enumerated(EnumType.STRING)
	@Column(nullable = false, updatable = false, length = 6)
	private Type entryType;

	@Column(nullable = false, updatable = false, precision = 19, scale = 4)
	private BigDecimal amount;

	@Column(nullable = false, updatable = false, precision = 19, scale = 4)
	private BigDecimal runningBalance;

	@Column(nullable = false, updatable = false)
	private Instant createdAt;

	protected LedgerEntry() {
	}

	LedgerEntry(UUID transactionId, Account account, Type entryType, BigDecimal amount, BigDecimal runningBalance, Instant createdAt) {
		this.transactionId = transactionId;
		this.account = account;
		this.entryType = entryType;
		this.amount = amount;
		this.runningBalance = runningBalance;
		this.createdAt = createdAt;
	}

	public UUID getTransactionId() {
		return transactionId;
	}

	public Account getAccount() {
		return account;
	}

	public Type getEntryType() {
		return entryType;
	}

	public BigDecimal getAmount() {
		return amount;
	}

	public BigDecimal getRunningBalance() {
		return runningBalance;
	}

	public Instant getCreatedAt() {
		return createdAt;
	}

}
