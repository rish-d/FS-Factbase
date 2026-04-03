import sys
import os
from loguru import logger

def setup_logger(log_file="logs/app.log", level="INFO"):
    """
    Configure loguru to output to both console and a file.
    Default: logs/app.log with rotation and retention.
    """
    # Remove default handler
    logger.remove()

    # Add console handler (standard error)
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}:{function}:{line}</cyan> - <level>{message}</level>",
        level=level,
        colorize=True
    )

    # Ensure log directory exists
    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    # Add file handler
    logger.add(
        log_file,
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
        level=level,
        rotation="5 MB",
        retention="10 days",
        compression="zip"
    )
    
    return logger

# Initialize default global logger
# This can be overridden by calling setup_logger again
setup_logger()

# Export logger for easy access
__all__ = ["logger", "setup_logger"]
