"""
General utility functions for the Tradier API client.
"""

from __future__ import annotations

import logging
from typing import Any, Optional


def log_for_level(
        logger,
        level: int,
        message: str,
        *,
        exc: Optional[BaseException] = None,
        exc_info: Any = None,
        stack_info: bool = False,
        extra: Optional[dict] = None,
):
    """Log *message* if *logger* is enabled for *level*.

    This is a tiny helper to avoid building/logging messages when the level is disabled.

    Args:
        logger: A ``logging.Logger`` instance.
        level: A stdlib logging level (e.g. ``logging.INFO``).
        message: The message string to log.
        exc: Optional exception instance to attach (equivalent to ``exc_info=exc``).
        exc_info: Passed through to ``logger.log(..., exc_info=...)``. If both ``exc`` and
            ``exc_info`` are provided, ``exc_info`` wins.
        stack_info: Passed through to the logger.
        extra: Passed through to the logger.
    """
    if not logger or not logger.isEnabledFor(level):
        return

    # Support passing an exception instance directly.
    if exc_info is None and exc is not None:
        exc_info = exc

    logger.log(level, message, exc_info=exc_info, stack_info=stack_info, extra=extra, stacklevel=2)


def log_entry_exit(level=logging.INFO):
    # A decorator to log entry and exit of a function at the specified level.
    def decorator(func):
        def wrapper(*args, **kwargs):
            logger = logging.getLogger(func.__qualname__)
            log_for_level(logger, level, f"Entering {func.__name__}")
            try:
                return func(*args, **kwargs)
            finally:
                log_for_level(logger, level, f"Finished {func.__name__}")

        return wrapper
    return decorator