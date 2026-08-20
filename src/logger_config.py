"""
StreamingLogger module for real-time file logging.
"""

import logging
import sys
from pathlib import Path
from typing import Optional


class StreamingLogger:
    """
    A streaming logger that writes log messages to a file in real-time.
    Follows Single Responsibility Principle.
    """

    def __init__(self, log_file: str, level: int = logging.INFO):
        """
        Initialize the streaming logger.

        Args:
            log_file: Path to the log file.
            level: Logging level (default: INFO).
        """
        self.log_file = Path(log_file)
        self.level = level
        self._logger = logging.getLogger("DataQualityFramework")
        self._logger.setLevel(level)
        self._logger.handlers = []  # Clear existing handlers

        # File handler with streaming mode
        file_handler = logging.FileHandler(self.log_file, mode="w", encoding="utf-8")
        file_handler.setLevel(level)

        # Formatter
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(formatter)

        # Add handler to logger
        self._logger.addHandler(file_handler)

        # Also log to console
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        self._logger.addHandler(console_handler)

    def log_info(self, message: str):
        """Log an info message."""
        self._logger.info(message)

    def log_warning(self, message: str):
        """Log a warning message."""
        self._logger.warning(message)

    def log_error(self, message: str):
        """Log an error message."""
        self._logger.error(message)

    def log_debug(self, message: str):
        """Log a debug message."""
        self._logger.debug(message)