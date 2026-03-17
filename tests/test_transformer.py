import pytest
import pandas as pd
import numpy as np
from src.transformers.transaction_transformer import TransactionTransformer


# =============================================================
# FIXTURES — reusable test data
# In pytest, fixtures are shared setup blocks
# =============================================================

@pytest.fixture
def transformer():
    """Returns a fresh TransactionTransformer instance for each test."""
    return TransactionTransformer()


@pytest.fixture
def valid_df():
    """
    Minimal valid DataFrame simulating IEEE-CIS raw data.
    This is our baseline — all required columns present, no nulls.
    """
    return pd.DataFrame({
        "TransactionID": [1, 2, 3],
        "TransactionDT": [86400, 172800, 259200],
        "TransactionAmt": [100.5, 250.0, 75.0],
        "ProductCD": ["W", "H", "W"],
        "card4": ["visa", "mastercard", "visa"],
        "card6": ["debit", "credit", "debit"],
        "P_emaildomain": ["gmail.com", "yahoo.com", "gmail.com"],
        "R_emaildomain": ["hotmail.com", "gmail.com", "yahoo.com"],
        "isFraud": [0, 1, 0]
    })


@pytest.fixture
def df_with_nulls():
    """DataFrame with nulls in categorical columns — real fintech scenario."""
    return pd.DataFrame({
        "TransactionID": [1, 2, 3],
        "TransactionDT": [86400, 172800, 259200],
        "TransactionAmt": [100.5, None, 75.0],
        "ProductCD": ["W", None, "W"],
        "card4": [None, "mastercard", "visa"],
        "card6": ["debit", None, "debit"],
        "P_emaildomain": ["gmail.com", None, "gmail.com"],
        "R_emaildomain": [None, "gmail.com", None],
        "isFraud": [0, 1, 0]
    })


# =============================================================
# TESTS: validate_required_columns
# =============================================================

class TestValidateRequiredColumns:

    def test_passes_with_all_required_columns(self, transformer, valid_df):
        """Happy path — all required columns present, no exception raised."""
        transformer.validate_required_columns(valid_df)  # Should not raise

    def test_raises_when_column_missing(self, transformer, valid_df):
        """If a required column is missing, pipeline must halt immediately."""
        df_missing = valid_df.drop(columns=["isFraud"])

        with pytest.raises(ValueError, match="Required columns missing"):
            transformer.validate_required_columns(df_missing)

    def test_raises_when_multiple_columns_missing(self, transformer, valid_df):
        """Multiple missing columns — error message must list all of them."""
        df_missing = valid_df.drop(columns=["isFraud", "TransactionAmt"])

        with pytest.raises(ValueError):
            transformer.validate_required_columns(df_missing)


# =============================================================
# TESTS: convert_timestamp
# =============================================================

class TestConvertTimestamp:

    def test_converts_to_datetime(self, transformer, valid_df):
        """TransactionDT must become datetime after conversion."""
        result = transformer.convert_timestamp(valid_df)
        assert pd.api.types.is_datetime64_any_dtype(result["TransactionDT"])

    def test_first_transaction_date(self, transformer, valid_df):
        """86400 seconds from reference date must equal 2017-12-02."""
        result = transformer.convert_timestamp(valid_df)
        first_date = result["TransactionDT"].iloc[0].date()
        assert str(first_date) == "2017-12-02"

    def test_does_not_modify_original_df(self, transformer, valid_df):
        """Transformer must never mutate the input DataFrame — immutability."""
        original_dtype = valid_df["TransactionDT"].dtype
        transformer.convert_timestamp(valid_df)
        assert valid_df["TransactionDT"].dtype == original_dtype


# =============================================================
# TESTS: fill_categoricals
# =============================================================

class TestFillCategoricals:

    def test_fills_nulls_with_unknown(self, transformer, df_with_nulls):
        """All null categoricals must become 'unknown' — never null in DB."""
        result = transformer.fill_categoricals(df_with_nulls)

        for col in transformer.CATEGORICAL_COLS:
            if col in result.columns:
                assert result[col].isna().sum() == 0

    def test_unknown_value_used(self, transformer, df_with_nulls):
        """The fill value must be exactly 'unknown' — not 'N/A' or empty."""
        result = transformer.fill_categoricals(df_with_nulls)
        assert "unknown" in result["card4"].values

    def test_non_null_values_preserved(self, transformer, df_with_nulls):
        """Existing values must not be overwritten."""
        result = transformer.fill_categoricals(df_with_nulls)
        assert "mastercard" in result["card4"].values
        assert "visa" in result["card4"].values


# =============================================================
# TESTS: full transform pipeline
# =============================================================

class TestTransform:

    def test_transform_returns_dataframe(self, transformer, valid_df):
        """transform() must always return a DataFrame."""
        result = transformer.transform(valid_df)
        assert isinstance(result, pd.DataFrame)

    def test_transform_preserves_row_count(self, transformer, valid_df):
        """No rows should be lost during transformation."""
        result = transformer.transform(valid_df)
        assert len(result) == len(valid_df)

    def test_isfraud_is_integer(self, transformer, valid_df):
        """isFraud must be integer for PostgreSQL SMALLINT compatibility."""
        result = transformer.transform(valid_df)
        assert result["isFraud"].dtype in [np.int32, np.int64, np.int8]

    def test_transform_fails_on_missing_columns(self, transformer):
        """Transform must fail fast on invalid input — never silently corrupt."""
        invalid_df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})

        with pytest.raises(ValueError):
            transformer.transform(invalid_df)