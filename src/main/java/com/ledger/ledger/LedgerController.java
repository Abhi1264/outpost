package com.ledger.ledger;

import com.ledger.ledger.LedgerApi.AccountView;
import com.ledger.ledger.LedgerApi.AmountRequest;
import com.ledger.ledger.LedgerApi.EntryView;
import com.ledger.ledger.LedgerApi.OpenAccountRequest;
import com.ledger.ledger.LedgerApi.TransactionView;
import com.ledger.ledger.LedgerApi.TransferRequest;
import jakarta.validation.Valid;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1")
class LedgerController {

	private final LedgerService ledger;

	LedgerController(LedgerService ledger) {
		this.ledger = ledger;
	}

	@PostMapping("/accounts")
	@ResponseStatus(HttpStatus.CREATED)
	AccountView openAccount(@Valid @RequestBody OpenAccountRequest request) {
		return ledger.openAccount(request.currency());
	}

	@GetMapping("/accounts/{accountNumber}")
	AccountView getAccount(@PathVariable String accountNumber) {
		return ledger.getAccount(accountNumber);
	}

	@GetMapping("/accounts/{accountNumber}/entries")
	List<EntryView> entries(@PathVariable String accountNumber, @PageableDefault(size = 50) Pageable pageable) {
		return ledger.entries(accountNumber, pageable);
	}

	@PostMapping("/accounts/{accountNumber}/deposits")
	@ResponseStatus(HttpStatus.CREATED)
	TransactionView deposit(@PathVariable String accountNumber, @Valid @RequestBody AmountRequest request) {
		return ledger.deposit(accountNumber, request.amount());
	}

	@PostMapping("/accounts/{accountNumber}/withdrawals")
	@ResponseStatus(HttpStatus.CREATED)
	TransactionView withdraw(@PathVariable String accountNumber, @Valid @RequestBody AmountRequest request) {
		return ledger.withdraw(accountNumber, request.amount());
	}

	@PostMapping("/transfers")
	@ResponseStatus(HttpStatus.CREATED)
	TransactionView transfer(@Valid @RequestBody TransferRequest request) {
		return ledger.transfer(request.fromAccountNumber(), request.toAccountNumber(), request.amount());
	}

}
