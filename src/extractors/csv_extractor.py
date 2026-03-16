import pandas as pd
from pathlib import Path
from src.extractors.base_extractor import BaseExtractor
from src.utils.logger import logger


class CsvExtractor(BaseExtractor):
    """
    Extractor for CSV files.

    Handles the IEEE-CIS Fraud Detection dataset:
        - train_transaction.csv (590k rows, 394 cols) — core banking simulation
        - train_identity.csv (144k rows, 41 cols) — KYC system simulation
    """

    def __init__(self, file_path: str, source_name: str):
        """
        Args:
            file_path: path to the CSV file
            source_name: identifier for logging
        """
        super().__init__(source_name)
        self.file_path = Path(file_path)

    def extract(self) -> pd.DataFrame:
        """
        Reads CSV file into a DataFrame.
        Uses pathlib.Path for cross-platform compatibility
        (Windows backslashes vs Linux forward slashes).
        """
        if not self.file_path.exists():
            logger.error(f"File not found: {self.file_path}")
            raise FileNotFoundError(f"Source file not found: {self.file_path}")

        logger.info(f"Reading CSV: {self.file_path}")
        df = pd.read_csv(self.file_path)
        logger.info(f"CSV loaded — shape: {df.shape}")
        return df

    def validate(self, df: pd.DataFrame) -> bool:
        """
        Basic validation for financial transaction files:
            - Must not be empty
            - Must have minimum expected columns
        """
        if df.empty:
            logger.error("Extracted DataFrame is empty")
            return False

        if len(df.columns) < 2:
            logger.error(f"Too few columns: {len(df.columns)}")
            return False

        logger.info(f"Validation passed — {len(df)} rows, {len(df.columns)} columns")
        return True