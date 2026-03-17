-- =============================================================
-- PROCEDURE 1: refresh_fraud_stats
-- Recalculates fraud statistics for a given month
-- Called nightly after daily transaction load
-- =============================================================

CREATE OR REPLACE PROCEDURE refresh_fraud_stats(target_month DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_total INTEGER;
    v_fraud_rate NUMERIC;
BEGIN
    SELECT COUNT(*) INTO v_total
    FROM transactions_partitioned
    WHERE DATE_TRUNC('month', "TransactionDT") = DATE_TRUNC('month', target_month);

    SELECT ROUND(SUM("isFraud") * 100.0 / COUNT(*), 2) INTO v_fraud_rate
    FROM transactions_partitioned
    WHERE DATE_TRUNC('month', "TransactionDT") = DATE_TRUNC('month', target_month);

    RAISE NOTICE 'Month: % | Total: % | Fraud rate: %', target_month, v_total, v_fraud_rate;
END;
$$;

-- =============================================================
-- PROCEDURE 2: flag_high_risk_transactions
-- Flags transactions above risk threshold for manual review
-- In production: feeds into case management system
-- =============================================================

CREATE OR REPLACE PROCEDURE flag_high_risk_transactions(
    amt_threshold NUMERIC DEFAULT 1000.00
)
LANGUAGE plpgsql
AS $$
DECLARE
    flagged_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO flagged_count
    FROM transactions_partitioned
    WHERE "TransactionAmt" > amt_threshold
      AND "isFraud" = 1;

    RAISE NOTICE 'Threshold: $ % | Flagged: %', amt_threshold, flagged_count;
END;
$$;