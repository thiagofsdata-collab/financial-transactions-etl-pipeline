import sys
from loguru import logger

# Remove default handler
logger.remove()

# Console handler — structured format for development
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{line}</cyan> — <level>{message}</level>",
    level="INFO",
    colorize=True,
)

# File handler — persists logs for audit trail
logger.add(
    "logs/pipeline_{time:YYYY-MM-DD}.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{line} — {message}",
    level="DEBUG",
    rotation="1 day",
    retention="30 days",
    compression="zip",
)