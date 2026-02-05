"""
Shared event loop for Celery async tasks.

This module provides a shared event loop that is reused across
Celery task invocations, avoiding the overhead and potential
resource leaks of creating a new loop for each task.
"""
import asyncio
import logging
from functools import wraps
from typing import Callable, Coroutine, Any, TypeVar

logger = logging.getLogger(__name__)

# Global shared event loop
_loop: asyncio.AbstractEventLoop | None = None

T = TypeVar("T")


def get_event_loop() -> asyncio.AbstractEventLoop:
    """
    Get or create a shared event loop for Celery tasks.

    This function ensures that the same event loop is reused across
    multiple Celery task invocations, which:
    - Reduces overhead of creating new loops
    - Allows proper cleanup of async resources
    - Prevents potential SQLAlchemy session conflicts

    Returns:
        asyncio.AbstractEventLoop: The shared event loop
    """
    global _loop

    if _loop is None or _loop.is_closed():
        _loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_loop)
        logger.debug("Created new shared event loop for Celery tasks")

    return _loop


def run_async(coro: Coroutine[Any, Any, T]) -> T:
    """
    Run an async coroutine in the shared event loop.

    This is the main entry point for running async code from
    synchronous Celery tasks.

    Args:
        coro: The coroutine to execute

    Returns:
        The result of the coroutine

    Example:
        @celery_app.task
        def my_sync_task():
            return run_async(my_async_function())
    """
    loop = get_event_loop()
    return loop.run_until_complete(coro)


def async_task(func: Callable[..., Coroutine[Any, Any, T]]) -> Callable[..., T]:
    """
    Decorator to convert an async function to a sync function
    that runs in the shared event loop.

    This is useful for wrapping async functions that will be
    called from Celery tasks.

    Args:
        func: The async function to wrap

    Returns:
        A synchronous wrapper function

    Example:
        @async_task
        async def fetch_data():
            async with aiohttp.ClientSession() as session:
                ...

        # Now can be called synchronously:
        result = fetch_data()
    """
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> T:
        return run_async(func(*args, **kwargs))
    return wrapper


def cleanup_event_loop() -> None:
    """
    Clean up the shared event loop.

    Call this when shutting down the Celery worker to ensure
    proper cleanup of async resources.
    """
    global _loop

    if _loop is not None and not _loop.is_closed():
        try:
            # Cancel all pending tasks
            pending = asyncio.all_tasks(_loop)
            for task in pending:
                task.cancel()

            # Run loop until all tasks are cancelled
            if pending:
                _loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )

            _loop.close()
            logger.info("Shared event loop cleaned up successfully")
        except Exception as e:
            logger.error(f"Error cleaning up event loop: {e}")
        finally:
            _loop = None
