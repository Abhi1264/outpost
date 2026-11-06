package com.ledger.ledger;

import com.ledger.IntegrationTestBase;
import com.ledger.ledger.LedgerApi.AccountView;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class LedgerAuditTest extends IntegrationTestBase {

	@Autowired
	LedgerAuditor auditor;

	@Test
	void reportsACorruptedAccountBalanceAndLeavesItRestored() {
		AccountView account = openAccount("USD");
		deposit(account.accountNumber(), money("10.0000"));
		BigDecimal original = jdbc.sql("select balance from accounts where account_number = ?")
				.param(account.accountNumber()).query(BigDecimal.class).single();
		try {
			jdbc.sql("update accounts set balance = ? where account_number = ?")
					.params(original.add(BigDecimal.ONE), account.accountNumber()).update();
			assertThat(auditor.audit()).anyMatch(m -> m.contains(account.accountNumber()));
		}
		finally {
			jdbc.sql("update accounts set balance = ? where account_number = ?")
					.params(original, account.accountNumber()).update();
		}
		assertThat(auditor.audit()).noneMatch(m -> m.contains(account.accountNumber()));
	}

}
