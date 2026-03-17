import time
from src.extractors.csv_extractor import CsvExtractor
from src.transformers.transaction_transformer import TransactionTransformer
from src.loaders.postgres_loader import PostgresLoader
from src.utils.logger import logger


class Pipeline:
    """
    Orchestrates the full ETL flow for financial transaction data.

    In production, this class would be triggered by:
        - Airflow DAG (enterprise fintech)
        - AWS Lambda + EventBridge (cloud-native)
        - Simple cron job (early-stage fintech)

    Current flow:
        1. Extract: reads raw CSV files (transaction + identity)
        2. Transform: cleans, types and enriches data
        3. Load: inserts into PostgreSQL running in Docker
    """

    def __init__(
        self,
        transaction_path: str,
        identity_path: str = None,
    ):
        self.transaction_path = transaction_path
        self.identity_path = identity_path

        self.transformer = TransactionTransformer()
        self.loader = PostgresLoader()

        logger.info("Pipeline initialized successfully")

    def _check_database(self) -> None:
        """
        Validates database connectivity before starting.
        Fail fast — better to stop here than after 10 minutes of processing.
        """
        logger.info("Checking database connectivity...")
        if not self.loader.test_connection():
            raise ConnectionError("Cannot connect to PostgreSQL — aborting pipeline")

    def run_transactions(self) -> dict:
        """
        Runs the ETL for the transaction file.
        Returns a metrics dict for monitoring and alerting.
        """
        metrics = {
            "source": self.transaction_path,
            "rows_extracted": 0,
            "rows_loaded": 0,
            "status": "failed",
            "duration_seconds": 0,
        }

        start_time = time.time()

        try:
            # --- EXTRACT ---
            logger.info("=" * 50)
            logger.info("STEP 1: EXTRACT")
            extractor = CsvExtractor(
                file_path=self.transaction_path,
                source_name="ieee_cis_transactions"
            )
            df_raw = extractor.run()
            metrics["rows_extracted"] = len(df_raw)

            # --- TRANSFORM ---
            logger.info("=" * 50)
            logger.info("STEP 2: TRANSFORM")
            df_clean = self.transformer.transform(df_raw)

            # --- LOAD ---
            logger.info("=" * 50)
            logger.info("STEP 3: LOAD")
            self.loader.load(
                df=df_clean,
                table_name="transactions",
                if_exists="replace",  # replace for first full load
                chunksize=1000,
            )
            metrics["rows_loaded"] = len(df_clean)
            metrics["status"] = "success"

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            metrics["status"] = "failed"
            raise

        finally:
            metrics["duration_seconds"] = round(time.time() - start_time, 2)
            logger.info(f"Pipeline metrics: {metrics}")

        return metrics

    def run(self) -> None:
        """
        Main entry point — runs the full pipeline.
        """
        logger.info("=" * 50)
        logger.info("FINANCIAL TRANSACTIONS ETL PIPELINE STARTING")
        logger.info("=" * 50)

        self._check_database()
        self.run_transactions()

        logger.info("=" * 50)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 50)


if __name__ == "__main__":
    pipeline = Pipeline(
        transaction_path="data/raw/train_transaction.csv",
        identity_path="data/raw/train_identity.csv",
    )
    pipeline.run()