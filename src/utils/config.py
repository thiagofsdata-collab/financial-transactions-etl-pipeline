from dotenv import load_dotenv
import os

# Loads all variables from .env into environment
load_dotenv()


class Config:
    """
    Centralizes all environment-based configuration.
    In production, these values come from secret managers
    (AWS Secrets Manager, Azure Key Vault) — never hardcoded.
    """

    # Database
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "")
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "")
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: str = os.getenv("POSTGRES_PORT", "5432")

    @property
    def database_url(self) -> str:
        """
        Builds the SQLAlchemy connection string.
        Format: postgresql+psycopg2://user:password@host:port/dbname
        """
        return (
            f"postgresql+psycopg2://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )


# Single instance — import this object everywhere
config = Config()