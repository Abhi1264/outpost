package com.ledger.ledger;

import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import java.util.Optional;
import java.util.UUID;

public interface AccountRepository extends JpaRepository<Account, UUID> {

	Optional<Account> findByAccountNumber(String accountNumber);

	@Lock(LockModeType.PESSIMISTIC_WRITE)
	@Query("select a from Account a where a.accountNumber = :accountNumber")
	Optional<Account> lockByAccountNumber(String accountNumber);

	@Modifying
	@Query(value = """
			insert into accounts (id, account_number, kind, balance, currency, version, created_at, updated_at)
			values (gen_random_uuid(), :accountNumber, 'SYSTEM', 0, :currency, 0, now(), now())
			on conflict (account_number) do nothing
			""", nativeQuery = true)
	void insertSystemAccountIfAbsent(String accountNumber, String currency);

}
