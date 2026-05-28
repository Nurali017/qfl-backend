import asyncio
import logging
from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.pool import NullPool

from app.config import get_settings

settings = get_settings()

def build_engine_kwargs(*, statement_timeout_ms: int | None = None) -> dict:
    engine_kwargs = {
        "echo": False,
        "pool_pre_ping": True,
        "pool_recycle": 1800,
    }

    if settings.database_pool_class == "null":
        engine_kwargs["poolclass"] = NullPool
    else:
        engine_kwargs["pool_size"] = settings.database_pool_size
        engine_kwargs["max_overflow"] = settings.database_max_overflow
        engine_kwargs["pool_timeout"] = 10

    if settings.database_url.startswith("postgresql+asyncpg://"):
        # idle_in_transaction_session_timeout: PG will FATAL-terminate a session that
        # stays in "idle in transaction" past this window. Backstop for sync tasks that
        # lose their session context (e.g. celery soft-time-limit fires mid-savepoint
        # loop) — prevents pool starvation that would block sync_best_players with
        # LockNotAvailableError on subsequent runs.
        # 15 min > celery task_time_limit (660s), so legit long tasks aren't killed.
        #
        # tcp_keepalives_*: server-side socket keepalives — if a celery worker process
        # is SIGKILL'd without a chance to send ROLLBACK, the server detects the dead
        # peer in ~60s (30s idle + 3×10s probes) and releases the transaction.
        server_settings: dict[str, str] = {
            "idle_in_transaction_session_timeout": str(settings.idle_in_transaction_timeout_ms),
            "tcp_keepalives_idle": "30",
            "tcp_keepalives_interval": "10",
            "tcp_keepalives_count": "3",
        }
        if settings.app_instance_name:
            server_settings["application_name"] = settings.app_instance_name
        if statement_timeout_ms is not None and statement_timeout_ms > 0:
            server_settings["statement_timeout"] = str(statement_timeout_ms)
        engine_kwargs["connect_args"] = {"server_settings": server_settings}

    return engine_kwargs


engine = create_async_engine(settings.database_url, **build_engine_kwargs())
web_engine = create_async_engine(
    settings.database_url,
    **build_engine_kwargs(statement_timeout_ms=settings.web_statement_timeout_ms),
)

AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)

WebAsyncSessionLocal = async_sessionmaker(
    web_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)


class Base(DeclarativeBase):
    pass


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with WebAsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


_pool_stats_logger = logging.getLogger("app.database.pool_stats")


async def log_pool_stats(interval_seconds: int = 60) -> None:
    """Periodically emit pool checkout metrics so the next QueuePool incident
    can be diagnosed from logs alone (size/checked_out/overflow per engine)."""
    while True:
        try:
            for name, eng in (("web", web_engine), ("worker", engine)):
                pool = eng.pool
                if hasattr(pool, "size") and hasattr(pool, "checkedout"):
                    _pool_stats_logger.info(
                        "pool=%s size=%d checked_in=%d checked_out=%d overflow=%d",
                        name,
                        pool.size(),
                        pool.checkedin(),
                        pool.checkedout(),
                        pool.overflow(),
                    )
        except Exception as exc:
            _pool_stats_logger.warning("pool stats failure: %s", exc)
        await asyncio.sleep(interval_seconds)
