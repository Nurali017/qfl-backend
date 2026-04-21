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

    if (
        statement_timeout_ms is not None
        and statement_timeout_ms > 0
        and settings.database_url.startswith("postgresql+asyncpg://")
    ):
        engine_kwargs["connect_args"] = {
            "server_settings": {
                "statement_timeout": str(statement_timeout_ms),
            }
        }

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
