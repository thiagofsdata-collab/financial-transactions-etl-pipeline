-- =============================================================
-- TABLE: transactions_partitioned
-- Partitioned by month for analytical performance
-- In production: each partition = one month of transactions
-- Query for December 2017 only scans that partition, not 590k rows
-- =============================================================

CREATE TABLE IF NOT EXISTS transactions_partitioned (
    "TransactionID"    BIGINT          NOT NULL,
    "TransactionDT"    TIMESTAMPTZ     NOT NULL,
    "TransactionAmt"   NUMERIC(12, 2),
    "ProductCD"        VARCHAR(10),
    "card4"            VARCHAR(20),
    "card6"            VARCHAR(20),
    "P_emaildomain"    VARCHAR(100),
    "R_emaildomain"    VARCHAR(100),
    "isFraud"          SMALLINT        NOT NULL DEFAULT 0,
    PRIMARY KEY ("TransactionID", "TransactionDT")
) PARTITION BY RANGE ("TransactionDT");

-- =============================================================
-- PARTITIONS: one per month in the dataset (Dec 2017 - Jun 2018)
-- =============================================================

CREATE TABLE IF NOT EXISTS transactions_2017_12
    PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2017-12-01') TO ('2018-01-01');

CREATE TABLE IF NOT EXISTS transactions_2018_01
    PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2018-01-01') TO ('2018-02-01');

CREATE TABLE IF NOT EXISTS transactions_2018_02
    PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2018-02-01') TO ('2018-03-01');

CREATE TABLE IF NOT EXISTS transactions_2018_03
    PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2018-03-01') TO ('2018-04-01');

CREATE TABLE IF NOT EXISTS transactions_2018_04
    PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2018-04-01') TO ('2018-05-01');

CREATE TABLE IF NOT EXISTS transactions_2018_05
    PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2018-05-01') TO ('2018-06-01');

CREATE TABLE IF NOT EXISTS transactions_2018_06
    PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2018-06-01') TO ('2018-07-01');

-- =============================================================
-- INDEXES: on fraud flag and amount for analytical queries
-- =============================================================

CREATE INDEX IF NOT EXISTS idx_fraud_flag
    ON transactions_partitioned ("isFraud");

CREATE INDEX IF NOT EXISTS idx_transaction_amt
    ON transactions_partitioned ("TransactionAmt");

CREATE INDEX IF NOT EXISTS idx_card4
    ON transactions_partitioned ("card4");