package com.ledger;

import org.springframework.boot.SpringApplication;

public class TestFinancialLedgerApplication {

	public static void main(String[] args) {
		SpringApplication.from(FinancialLedgerApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
