package com.ledger.ledger;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.UUID;

public interface LedgerEntryRepository extends JpaRepository<LedgerEntry, UUID> {

	@Query("select e from LedgerEntry e join fetch e.account a where a.accountNumber = :accountNumber order by e.createdAt desc")
	List<LedgerEntry> findByAccountNumber(String accountNumber, Pageable pageable);

}
