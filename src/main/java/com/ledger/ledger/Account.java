package com.ledger.ledger;

import com.ledger.common.LedgerException;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "accounts")
public class Account {

	public enum Kind { CUSTOMER, SYSTEM }

	@Id
	@GeneratedValue
	private UUID id;

	@Column(nullable = false, updatable = false, length = 32)
	private String accountNumber;

	@Enumerated(EnumType.STRING)
	@Column(nullable = false, updatable = false, length = 16)
	private Kind kind;

	@Column(nullable = false, precision = 19, scale = 4)
	private BigDecimal balance;

	@Column(nullable = false, updatable = false, length = 3)
	private String currency;

	@Version
	private long version;

	@Column(nullable = false, updatable = false)
	private Instant createdAt;

	@Column(nullable = false)
	private Instant updatedAt;

	protected Account() {
	}

	Account(String accountNumber, Kind kind, String currency) {
		this.accountNumber = accountNumber;
		this.kind = kind;
		this.currency = currency;
		this.balance = Money.ZERO;
		this.createdAt = Instant.now();
		this.updatedAt = this.createdAt;
	}

	BigDecimal credit(BigDecimal amount) {
		balance = Money.of(balance.add(amount));
		return balance;
	}

	BigDecimal debit(BigDecimal amount) {
		BigDecimal next = Money.of(balance.subtract(amount));
		if (kind == Kind.CUSTOMER && next.signum() < 0) {
			throw LedgerException.insufficientFunds(accountNumber);
		}
		balance = next;
		return balance;
	}

	@PreUpdate
	void touch() {
		updatedAt = Instant.now();
	}

	public UUID getId() {
		return id;
	}

	public String getAccountNumber() {
		return accountNumber;
	}

	public Kind getKind() {
		return kind;
	}

	public BigDecimal getBalance() {
		return balance;
	}

	public String getCurrency() {
		return currency;
	}

	public Instant getCreatedAt() {
		return createdAt;
	}

}
