package com.ledger.ledger;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public final class LedgerApi {

	private LedgerApi() {
	}

	public enum TransactionType { DEPOSIT, WITHDRAWAL, TRANSFER }

	public record OpenAccountRequest(@NotBlank @Pattern(regexp = "^[A-Z]{3}$") String currency) {
	}

	public record AmountRequest(
			@NotNull @DecimalMin("0.0001") @Digits(integer = 15, fraction = Money.SCALE) BigDecimal amount) {
	}

	public record TransferRequest(
			@NotBlank String fromAccountNumber,
			@NotBlank String toAccountNumber,
			@NotNull @DecimalMin("0.0001") @Digits(integer = 15, fraction = Money.SCALE) BigDecimal amount) {
	}

	public record AccountView(UUID id, String accountNumber, String currency, BigDecimal balance, Instant createdAt) {

		static AccountView from(Account account) {
			return new AccountView(account.getId(), account.getAccountNumber(), account.getCurrency(),
					account.getBalance(), account.getCreatedAt());
		}
	}

	public record EntryView(String accountNumber, LedgerEntry.Type entryType, BigDecimal amount,
			BigDecimal runningBalance, Instant createdAt) {

		static EntryView from(LedgerEntry entry) {
			return new EntryView(entry.getAccount().getAccountNumber(), entry.getEntryType(), entry.getAmount(),
					entry.getRunningBalance(), entry.getCreatedAt());
		}
	}

	public record TransactionView(UUID transactionId, TransactionType type, String currency, BigDecimal amount,
			Instant occurredAt, List<EntryView> entries) {
	}

}
