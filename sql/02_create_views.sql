-- =============================================================
-- VIEW 1: vw_fraud_summary
-- Monthly fraud summary for executive dashboards
-- Business question: "How is fraud evolving month over month?"
-- =============================================================

CREATE OR REPLACE VIEW vw_fraud_summary AS
SELECT
    DATE_TRUNC('month', "TransactionDT") AS transaction_month,
    COUNT(*) AS total_transactions,
    SUM("isFraud") AS total_fraud,
    ROUND(
        SUM("isFraud") * 100.0 / COUNT(*), 2
    ) AS fraud_rate_pct,
    ROUND(AVG("TransactionAmt")::NUMERIC, 2) AS avg_transaction_amt,
    ROUND(SUM(CASE WHEN "isFraud" = 1
        THEN "TransactionAmt" ELSE 0 END)::NUMERIC, 2) AS total_fraud_amt
FROM transactions_partitioned
GROUP BY DATE_TRUNC('month', "TransactionDT")
ORDER BY transaction_month;

-- =============================================================
-- VIEW 2: vw_fraud_by_card
-- Fraud breakdown by card type
-- Business question: "Which card network has highest fraud rate?"
-- =============================================================

CREATE OR REPLACE VIEW vw_fraud_by_card AS
SELECT
    "card4" AS card_network,
    "card6" AS card_type,
    COUNT(*) AS total_transactions,
    SUM("isFraud") AS total_fraud,
    ROUND(
        SUM("isFraud") * 100.0 / COUNT(*), 2
    ) AS fraud_rate_pct,
    ROUND(AVG("TransactionAmt")::NUMERIC, 2) AS avg_amt,
    ROUND(MAX("TransactionAmt")::NUMERIC, 2) AS max_amt
FROM transactions_partitioned
GROUP BY "card4", "card6"
ORDER BY fraud_rate_pct DESC;

-- =============================================================
-- VIEW 3: vw_high_value_fraud
-- High value fraudulent transactions
-- Business question: "What does a high-value fraud look like?"
-- =============================================================

CREATE OR REPLACE VIEW vw_high_value_fraud AS
SELECT
    "TransactionID",
    "TransactionDT",
    "TransactionAmt",
    "card4",
    "card6",
    "ProductCD",
    "P_emaildomain"
FROM transactions_partitioned
WHERE "isFraud" = 1
  AND "TransactionAmt" > (
      SELECT PERCENTILE_CONT(0.95)
             WITHIN GROUP (ORDER BY "TransactionAmt")
      FROM transactions_partitioned
      WHERE "isFraud" = 1
  )
ORDER BY "TransactionAmt" DESC;