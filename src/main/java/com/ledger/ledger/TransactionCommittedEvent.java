package com.ledger.ledger;

import java.util.List;
import java.util.UUID;

public record TransactionCommittedEvent(UUID transactionId, List<UUID> outboxIds) {
}
