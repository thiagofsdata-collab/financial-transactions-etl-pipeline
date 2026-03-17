import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from src.utils.config import config
from src.utils.logger import logger


class PostgresLoader:
    """
    Loads transformed DataFrames into PostgreSQL running in Docker.

    In production fintech pipelines, this layer handles:
        - Connection pooling for high-throughput transaction ingestion
        - Upsert logic to avoid duplicate transactions
        - Bulk inserts optimized for 590k+ row datasets
    """

    def __init__(self):
        self.engine: Engine = self._create_engine()

    def _create_engine(self) -> Engine:
        """
        Creates SQLAlchemy engine from config.
        Engine manages connection pooling automatically —
        critical for pipelines processing millions of transactions.
        """
        try:
            engine = create_engine(
                config.database_url,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
            )
            logger.info("SQLAlchemy engine created successfully")
            return engine
        except Exception as e:
            logger.error(f"Failed to create database engine: {e}")
            raise

    def test_connection(self) -> bool:
        """
        Validates database connectivity before attempting any load.
        In production, this runs as a health check before pipeline start.
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection test passed")
            return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False

    def load(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "append",
        chunksize: int = 1000,
    ) -> None:
        """
        Loads DataFrame into PostgreSQL table.

        Args:
            df: transformed DataFrame ready for storage
            table_name: target table in PostgreSQL
            if_exists: 'append' (default) or 'replace'
                - append: adds rows — use for incremental daily loads
                - replace: drops and recreates — use for full reloads
            chunksize: rows per batch — 1000 is safe for most setups.
                In production with 590k rows, tune this based on
                available memory and network latency to the DB.
        """
        try:
            logger.info(f"Loading {len(df)} rows into table: {table_name}")

            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists=if_exists,
                index=False,
                chunksize=chunksize,
                method="multi",
            )

            logger.info(f"Load complete — {len(df)} rows inserted into {table_name}")

        except Exception as e:
            logger.error(f"Load failed for table {table_name}: {e}")
            raise

    def execute_sql(self, sql: str) -> None:
        """
        Executes raw SQL — used for CREATE TABLE, CREATE INDEX, etc.
        Wrapped with logging for full audit trail.
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()
            logger.info("SQL executed successfully")
        except Exception as e:
            logger.error(f"SQL execution failed: {e}")
            raise