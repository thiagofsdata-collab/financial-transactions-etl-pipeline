from abc import ABC, abstractmethod
import pandas as pd
from src.utils.logger import logger


class BaseExtractor(ABC):
    """
    Abstract base class for all data extractors.

    In a financial pipeline, data arrives from multiple sources:
    core banking (CSV/DB), card processors (API), PIX (Kafka).
    This class enforces a consistent interface across all of them.

    Every extractor MUST implement:
        - extract(): reads raw data from the source
        - validate(): checks data quality before transformation
    """

    def __init__(self, source_name: str):
        """
        Args:
            source_name: identifier for logging (e.g. 'ieee_cis_transactions')
        """
        self.source_name = source_name
        logger.info(f"Initializing extractor for source: {source_name}")

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """
        Reads raw data from the source and returns a DataFrame.
        Must be implemented by every subclass.
        """
        pass

    @abstractmethod
    def validate(self, df: pd.DataFrame) -> bool:
        """
        Validates the extracted DataFrame before passing to transformer.
        Must be implemented by every subclass.

        Returns:
            True if data is valid, False otherwise
        """
        pass

    def run(self) -> pd.DataFrame:
        """
        Orchestrates the extraction flow: extract → validate.
        This method is the same for ALL extractors — defined once here.

        In production, this is where you'd add:
            - retry logic for API failures
            - circuit breakers for downstream protection
            - metrics emission to Datadog/Prometheus
        """
        logger.info(f"Starting extraction: {self.source_name}")

        df = self.extract()

        logger.info(f"Extracted {len(df)} rows from {self.source_name}")

        is_valid = self.validate(df)

        if not is_valid:
            logger.error(f"Validation failed for source: {self.source_name}")
            raise ValueError(f"Data validation failed for {self.source_name}")

        logger.info(f"Validation passed for source: {self.source_name}")
        return df