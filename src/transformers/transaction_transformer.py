import pandas as pd
import numpy as np
from datetime import datetime, timezone
from src.utils.logger import logger


class TransactionTransformer:
    """
    Transforms raw IEEE-CIS transaction data for loading into PostgreSQL.

    Mirrors a real fintech transformation layer where raw data from
    core banking systems needs standardization before analytics or
    fraud model scoring.
    """

    # Reference date for TransactionDT conversion
    # IEEE-CIS TransactionDT is seconds elapsed from this point
    REFERENCE_DATE = datetime(2017, 12, 1, tzinfo=timezone.utc)

    # Categorical columns that must never be null in the database
    CATEGORICAL_COLS = ["ProductCD", "card4", "card6", "P_emaildomain", "R_emaildomain"]

    # Core financial columns — pipeline fails if these are missing
    REQUIRED_COLS = ["TransactionID", "TransactionDT", "TransactionAmt", "isFraud"]

    def __init__(self):
        logger.info("TransactionTransformer initialized")

    def validate_required_columns(self, df: pd.DataFrame) -> None:
        """
        Fails fast if critical columns are missing.
        In production, missing core columns = pipeline halt + alert.
        """
        missing = [col for col in self.REQUIRED_COLS if col not in df.columns]
        if missing:
            logger.error(f"Missing required columns: {missing}")
            raise ValueError(f"Required columns missing: {missing}")
        logger.info("Required columns validated successfully")

    def convert_timestamp(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Converts TransactionDT from relative Unix seconds to real datetime.

        IEEE-CIS TransactionDT is seconds elapsed since 2017-12-01.
        In real core banking systems, timestamps often arrive in
        proprietary formats that need normalization.
        """
        df = df.copy()
        df["TransactionDT"] = pd.to_datetime(
            df["TransactionDT"].apply(
                lambda x: self.REFERENCE_DATE.timestamp() + x
            ),
            unit="s",
            utc=True
        )
        logger.info("TransactionDT converted to UTC datetime")
        return df

    def clean_amounts(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ensures TransactionAmt is float and flags anomalous values.

        In fraud detection, extreme transaction amounts are a signal.
        We preserve them but ensure correct typing for SQL storage.
        """
        df = df.copy()
        df["TransactionAmt"] = pd.to_numeric(df["TransactionAmt"], errors="coerce")

        null_amounts = df["TransactionAmt"].isna().sum()
        if null_amounts > 0:
            logger.warning(f"Found {null_amounts} null TransactionAmt values")

        negative_amounts = (df["TransactionAmt"] < 0).sum()
        if negative_amounts > 0:
            logger.warning(f"Found {negative_amounts} negative TransactionAmt values")

        return df

    def fill_categoricals(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fills null categorical columns with 'unknown'.

        Null card types or email domains break GROUP BY queries
        in SQL analytical layers — we make nulls explicit instead.
        """
        df = df.copy()
        existing_cols = [col for col in self.CATEGORICAL_COLS if col in df.columns]
        for col in existing_cols:
            null_count = df[col].isna().sum()
            if null_count > 0:
                logger.info(f"Filling {null_count} nulls in column: {col}")
            df[col] = df[col].fillna("unknown")
        return df

    def enforce_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enforces correct dtypes for PostgreSQL compatibility.
        SQLAlchemy maps Python/Pandas dtypes to SQL types —
        mismatches cause silent truncation or load failures.
        """
        df = df.copy()
        df["TransactionID"] = df["TransactionID"].astype(int)
        df["isFraud"] = df["isFraud"].astype(int)
        logger.info("Column types enforced for PostgreSQL compatibility")
        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Orchestrates the full transformation pipeline.
        Order matters: validate → timestamp → amounts → categoricals → types.
        """
        logger.info(f"Starting transformation — input shape: {df.shape}")

        self.validate_required_columns(df)
        df = self.convert_timestamp(df)
        df = self.clean_amounts(df)
        df = self.fill_categoricals(df)
        df = self.enforce_types(df)

        logger.info(f"Transformation complete — output shape: {df.shape}")
        return df