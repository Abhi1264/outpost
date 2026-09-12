-- Accounts. Customer accounts can never go negative (enforced here as the last line of defence,
-- and in LedgerService under a row lock). SYSTEM accounts (one clearing account per currency)
-- mirror money entering/leaving the ledger and are allowed to be negative.
CREATE TABLE accounts (
    id             UUID PRIMARY KEY,
    account_number VARCHAR(32)   NOT NULL,
    kind           VARCHAR(16)   NOT NULL CHECK (kind IN ('CUSTOMER', 'SYSTEM')),
    balance        NUMERIC(19,4) NOT NULL DEFAULT 0,
    currency       VARCHAR(3)    NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    version        BIGINT        NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT chk_accounts_customer_non_negative CHECK (kind = 'SYSTEM' OR balance >= 0)
);
CREATE UNIQUE INDEX ux_accounts_account_number ON accounts (account_number);

-- Double-entry journal. Every transaction_id has exactly one DEBIT and one CREDIT of equal amount.
-- DEBIT decreases the account balance, CREDIT increases it.
CREATE TABLE ledger_entries (
    id              UUID PRIMARY KEY,
    transaction_id  UUID          NOT NULL,
    account_id      UUID          NOT NULL REFERENCES accounts (id),
    entry_type      VARCHAR(6)    NOT NULL CHECK (entry_type IN ('DEBIT', 'CREDIT')),
    amount          NUMERIC(19,4) NOT NULL CHECK (amount > 0),
    running_balance NUMERIC(19,4) NOT NULL,
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT now()
);
CREATE INDEX ix_ledger_entries_transaction_id ON ledger_entries (transaction_id);
CREATE INDEX ix_ledger_entries_account_created ON ledger_entries (account_id, created_at DESC);

CREATE TABLE webhook_subscriptions (
    id         UUID PRIMARY KEY,
    client_id  VARCHAR(128)  NOT NULL,
    target_url VARCHAR(2048) NOT NULL,
    secret_key VARCHAR(128)  NOT NULL,
    is_active  BOOLEAN       NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ   NOT NULL DEFAULT now()
);
CREATE INDEX ix_webhook_subscriptions_client ON webhook_subscriptions (client_id);
CREATE INDEX ix_webhook_subscriptions_active ON webhook_subscriptions (is_active) WHERE is_active;

-- Transactional outbox: written in the same transaction as ledger_entries, drained asynchronously.
CREATE TABLE webhook_outbox (
    id            UUID PRIMARY KEY,
    event_type    VARCHAR(64)   NOT NULL,
    payload       JSONB         NOT NULL,
    target_url    VARCHAR(2048) NOT NULL,
    secret_key    VARCHAR(128)  NOT NULL,
    status        VARCHAR(16)   NOT NULL CHECK (status IN ('PENDING', 'PROCESSING', 'DELIVERED', 'FAILED')),
    retry_count   INT           NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMPTZ   NOT NULL,
    last_error    TEXT,
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ   NOT NULL DEFAULT now()
);
CREATE INDEX ix_webhook_outbox_due ON webhook_outbox (next_retry_at) WHERE status IN ('PENDING', 'PROCESSING');

-- Idempotency store. key is "<principal>:<Idempotency-Key header>".
CREATE TABLE idempotency_keys (
    key              VARCHAR(255) PRIMARY KEY,
    request_hash     CHAR(64)     NOT NULL,
    status_code      INT          NOT NULL,
    response_payload JSONB,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX ix_idempotency_keys_created_at ON idempotency_keys (created_at);
