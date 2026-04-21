from app import database
from app.api import deps


def test_build_engine_kwargs_adds_asyncpg_statement_timeout(monkeypatch):
    monkeypatch.setattr(database.settings, "database_url", "postgresql+asyncpg://user:pass@localhost/db")

    kwargs = database.build_engine_kwargs(statement_timeout_ms=123)

    assert kwargs["connect_args"]["server_settings"]["statement_timeout"] == "123"


def test_build_engine_kwargs_skips_statement_timeout_for_non_asyncpg(monkeypatch):
    monkeypatch.setattr(database.settings, "database_url", "sqlite+aiosqlite:///:memory:")

    kwargs = database.build_engine_kwargs(statement_timeout_ms=123)

    assert "connect_args" not in kwargs


def test_api_deps_reexport_database_get_db():
    assert deps.get_db is database.get_db
