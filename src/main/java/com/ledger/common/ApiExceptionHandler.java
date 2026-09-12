package com.ledger.common;

import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
class ApiExceptionHandler {

	@ExceptionHandler(LedgerException.class)
	ProblemDetail ledger(LedgerException ex) {
		return ProblemDetail.forStatusAndDetail(ex.status(), ex.getMessage());
	}

	@ExceptionHandler(DataIntegrityViolationException.class)
	ProblemDetail integrity(DataIntegrityViolationException ex) {
		return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, "Request violates a data constraint");
	}

	@ExceptionHandler(ConcurrencyFailureException.class)
	ProblemDetail concurrency(ConcurrencyFailureException ex) {
		return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, "Concurrent modification detected, retry the request");
	}

}
