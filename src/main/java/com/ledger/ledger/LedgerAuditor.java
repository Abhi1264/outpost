package com.ledger.ledger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
class LedgerAuditor {

	private static final Logger log = LoggerFactory.getLogger(LedgerAuditor.class);

	private final JdbcClient jdbc;

	LedgerAuditor(JdbcClient jdbc) {
		this.jdbc = jdbc;
	}

	@Scheduled(cron = "0 0 * * * *")
	void scheduled() {
		audit();
	}

	List<String> audit() {
		List<String> mismatches = new ArrayList<>();
		jdbc.sql("""
				select a.account_number
				from accounts a
				left join ledger_entries e on e.account_id = a.id
				group by a.id, a.account_number, a.balance
				having a.balance <> coalesce(sum(case when e.entry_type = 'CREDIT' then e.amount else -e.amount end), 0)
				""").query(String.class).list().forEach(n -> mismatches.add("balance != journal net for " + n));
		jdbc.sql("select currency from accounts group by currency having sum(balance) <> 0").query(String.class).list()
				.forEach(c -> mismatches.add("currency " + c + " does not sum to 0"));
		jdbc.sql("""
				select a.account_number
				from (
				    select account_id, running_balance,
				           coalesce(lag(running_balance) over (partition by account_id order by created_at, id), 0)
				           + case when entry_type = 'CREDIT' then amount else -amount end as expected
				    from ledger_entries
				) c
				join accounts a on a.id = c.account_id
				where c.running_balance <> c.expected
				""").query(String.class).list().forEach(n -> mismatches.add("running balance chain broken for " + n));
		mismatches.forEach(m -> log.warn("Ledger mismatch: {}", m));
		return mismatches;
	}

}
