package com.ledger.common;

import org.springframework.http.HttpStatus;

public class LedgerException extends RuntimeException {

	private final HttpStatus status;

	public LedgerException(HttpStatus status, String message) {
		super(message);
		this.status = status;
	}

	public HttpStatus status() {
		return status;
	}

	public static LedgerException accountNotFound(String accountNumber) {
		return new LedgerException(HttpStatus.NOT_FOUND, "Account " + accountNumber + " not found");
	}

	public static LedgerException insufficientFunds(String accountNumber) {
		return new LedgerException(HttpStatus.UNPROCESSABLE_CONTENT, "Insufficient funds in account " + accountNumber);
	}

	public static LedgerException currencyMismatch(String from, String to) {
		return new LedgerException(HttpStatus.UNPROCESSABLE_CONTENT, "Currency mismatch: " + from + " vs " + to);
	}

	public static LedgerException invalid(String message) {
		return new LedgerException(HttpStatus.BAD_REQUEST, message);
	}

}
