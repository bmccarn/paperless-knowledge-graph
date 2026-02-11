"""Shared retry utilities with exponential backoff for transient errors."""

import asyncio
import logging
import random

from openai import APIConnectionError, APITimeoutError, RateLimitError, APIStatusError

logger = logging.getLogger(__name__)

# Transient HTTP status codes worth retrying
_TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}


def _is_transient(exc: Exception) -> bool:
    """Check if an exception is transient and worth retrying."""
    if isinstance(exc, (APIConnectionError, APITimeoutError, RateLimitError)):
        return True
    if isinstance(exc, APIStatusError) and exc.status_code in _TRANSIENT_STATUS_CODES:
        return True
    if isinstance(exc, (ConnectionError, TimeoutError, OSError)):
        return True
    # asyncpg / neo4j transient connection errors
    exc_name = type(exc).__name__
    if any(keyword in exc_name for keyword in (
        "ConnectionRefused", "ConnectionReset", "InterfaceError",
        "ConnectionDoesNotExist", "ServiceUnavailable", "SessionExpired",
    )):
        return True
    exc_str = str(exc).lower()
    if any(kw in exc_str for kw in ("connection", "timeout", "unavailable", "reset", "refused")):
        return True
    return False


async def retry_with_backoff(
    fn,
    max_retries: int = 3,
    base_delay: float = 2.0,
    max_delay: float = 30.0,
    operation: str = "",
):
    """Retry an async callable with exponential backoff + jitter.
    
    Only retries transient errors; non-transient errors are raised immediately.
    """
    for attempt in range(max_retries + 1):
        try:
            return await fn()
        except Exception as e:
            if not _is_transient(e):
                raise
            if attempt == max_retries:
                raise
            delay = min(base_delay * (2 ** attempt) + random.uniform(0, 1), max_delay)
            op_str = f" [{operation}]" if operation else ""
            logger.warning(
                f"Attempt {attempt + 1}/{max_retries + 1}{op_str} failed: {e}. "
                f"Retrying in {delay:.1f}s..."
            )
            await asyncio.sleep(delay)


async def retry_db(fn, operation: str = ""):
    """Retry a DB operation with shorter backoff (2 attempts, 1s base delay)."""
    return await retry_with_backoff(
        fn, max_retries=2, base_delay=1.0, max_delay=5.0, operation=operation
    )
