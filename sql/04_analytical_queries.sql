-- =============================================================
-- QUERY 1: LAG — Transaction amount vs previous transaction
-- Business question: "Did the transaction amount spike suddenly?"
-- A sudden spike is a classic fraud signal
-- =============================================================

SELECT
    "TransactionID",
    "TransactionDT",
    "TransactionAmt",
    "card4",
    "isFraud",

    LAG("TransactionAmt")
        OVER (
            PARTITION BY "card4"
            ORDER BY "TransactionDT"
        ) AS prev_transaction_amt,

    "TransactionAmt" - LAG("TransactionAmt")
        OVER (
            PARTITION BY "card4"
            ORDER BY "TransactionDT"
        ) AS amt_delta,

    ROUND(
        "TransactionAmt" / NULLIF(
            LAG("TransactionAmt")
            OVER (
                PARTITION BY "card4"
                ORDER BY "TransactionDT"
            ), 0
        ), 2
    ) AS amt_ratio_vs_prev

FROM transactions_partitioned
WHERE "card4" != 'unknown'
ORDER BY "TransactionDT"
LIMIT 100;


-- =============================================================
-- QUERY 2: LEAD — Detect small transaction before large fraud
-- Business question: "Is this small transaction a probe?"
-- Fraudsters often test cards with small amounts first
-- =============================================================

SELECT
    "TransactionID",
    "TransactionDT",
    "TransactionAmt",
    "card4",
    "isFraud",

    LEAD("TransactionAmt")
        OVER (
            PARTITION BY "card4"
            ORDER BY "TransactionDT"
        ) AS next_transaction_amt,

    LEAD("isFraud")
        OVER (
            PARTITION BY "card4"
            ORDER BY "TransactionDT"
        ) AS next_is_fraud

FROM transactions_partitioned
WHERE "card4" != 'unknown'
ORDER BY "TransactionDT"
LIMIT 100;


-- =============================================================
-- QUERY 3: RANK — Top 5 highest fraud transactions per month
-- Business question: "What are the biggest fraud cases each month?"
-- =============================================================

SELECT *
FROM (
    SELECT
        "TransactionID",
        "TransactionDT",
        "TransactionAmt",
        "card4",
        DATE_TRUNC('month', "TransactionDT") AS transaction_month,

        RANK()
            OVER (
                PARTITION BY DATE_TRUNC('month', "TransactionDT")
                ORDER BY "TransactionAmt" DESC
            ) AS rank_in_month

    FROM transactions_partitioned
    WHERE "isFraud" = 1
) ranked
WHERE rank_in_month <= 5
ORDER BY transaction_month, rank_in_month;


-- =============================================================
-- QUERY 4: Running total — Cumulative fraud amount per month
-- Business question: "How is fraud exposure accumulating?"
-- Classic accounting concept applied to fraud monitoring
-- =============================================================

SELECT
    "TransactionID",
    "TransactionDT",
    "TransactionAmt",
    "isFraud",

    SUM(CASE WHEN "isFraud" = 1 THEN "TransactionAmt" ELSE 0 END)
        OVER (
            ORDER BY "TransactionDT"
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_fraud_amt,

    SUM("isFraud")
        OVER (
            ORDER BY "TransactionDT"
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_fraud_count

FROM transactions_partitioned
ORDER BY "TransactionDT"
LIMIT 100;