# Financial Transactions ETL Pipeline

![CI](https://github.com/thiagofsdata-collab/financial-transactions-etl-pipeline/actions/workflows/ci.yml/badge.svg)

A production-grade ETL pipeline for financial transaction data, built with Python, PostgreSQL and Docker. Simulates a real fintech architecture processing 590k+ transactions with fraud detection analytics.

---

## Architecture
```
IEEE-CIS CSV Files
      ↓
CsvExtractor (OOP/ABC)
      ↓
TransactionTransformer (type enforcement, null handling)
      ↓
PostgresLoader (COPY protocol — 10x faster than INSERT)
      ↓
PostgreSQL 15 (Docker) — partitioned by month
      ↓
SQL Analytics (Views, Procedures, Window Functions)
      ↓
EDA Notebook (Plotly)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.13 |
| Database | PostgreSQL 15 (Docker) |
| ORM | SQLAlchemy 2.0 |
| Data Processing | Pandas, NumPy |
| Visualization | Plotly |
| Testing | pytest |
| CI/CD | GitHub Actions |
| Logging | Loguru |

---

## Dataset

IEEE-CIS Fraud Detection (Kaggle)
- `train_transaction.csv` — 590k rows, 394 columns (core banking simulation)
- `train_identity.csv` — 144k rows, 41 columns (KYC system simulation)
- Fraud rate: 3.5% — severe class imbalance

Download: https://www.kaggle.com/competitions/ieee-fraud-detection/data

---

## Project Structure
```
financial-transactions-etl-pipeline/
  src/
    extractors/       ← OOP base class + CSV implementation
    transformers/     ← type enforcement, null handling
    loaders/          ← PostgreSQL COPY protocol
    utils/            ← logger, config
    pipeline.py       ← ETL orchestrator
  sql/
    01_create_tables.sql    ← partitioned table by month
    02_create_views.sql     ← analytical views
    03_procedures.sql       ← stored procedures
    04_analytical_queries.sql ← LAG, LEAD, RANK
  notebooks/
    01_eda_report.ipynb     ← Plotly fraud analysis
  tests/
    test_transformer.py     ← 13 pytest tests
  .github/workflows/
    ci.yml                  ← GitHub Actions CI
  docker-compose.yml
  requirements.txt
  .env.example
```

---

## Setup

### Prerequisites
- Python 3.13
- Docker Desktop
- Kaggle account (for dataset)

### Installation
```bash
# Clone the repository
git clone https://github.com/thiagofsdata-collab/financial-transactions-etl-pipeline.git
cd financial-transactions-etl-pipeline

# Create virtual environment
python -m venv .venv
.venv\Scripts\Activate.ps1  # Windows
source .venv/bin/activate    # Linux/Mac

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your credentials
```

### Database
```bash
# Start PostgreSQL container
docker-compose up -d

# Verify container is healthy
docker ps
```

### Run Pipeline
```bash
python -m src.pipeline
```

### Run Tests
```bash
pytest tests/ -v
```

---

## SQL Analytics
```sql
-- Monthly fraud evolution
SELECT * FROM vw_fraud_summary;

-- Fraud by card network
SELECT * FROM vw_fraud_by_card;

-- Top 5 fraud cases per month (RANK)
SELECT * FROM (
    SELECT *, RANK() OVER (
        PARTITION BY DATE_TRUNC('month', "TransactionDT")
        ORDER BY "TransactionAmt" DESC
    ) AS rank_in_month
    FROM transactions_partitioned
    WHERE "isFraud" = 1
) r WHERE rank_in_month <= 5;

-- Stored procedures
CALL refresh_fraud_stats('2018-01-01');
CALL flag_high_risk_transactions(1000.00);
```

---

## Key Results

| Metric | Value |
|---|---|
| Total transactions processed | 590,540 |
| Load time (COPY protocol) | ~2 minutes |
| Fraud rate | 3.50% |
| Total fraud exposure | $3.08M |
| Test coverage | 13 tests passing |
| CI/CD | GitHub Actions ✅ |

---

## Author

Thiago Feliciano — [Meu Linkedin](https://www.linkedin.com/in/thiago-feliciano-40a1461b9/)
