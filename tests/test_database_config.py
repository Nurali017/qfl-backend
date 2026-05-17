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


def test_build_engine_kwargs_sets_idle_in_transaction_timeout(monkeypatch):
    monkeypatch.setattr(database.settings, "database_url", "postgresql+asyncpg://u:p@h/d")
    monkeypatch.setattr(database.settings, "idle_in_transaction_timeout_ms", 900_000)

    kwargs = database.build_engine_kwargs()

    server_settings = kwargs["connect_args"]["server_settings"]
    assert server_settings["idle_in_transaction_session_timeout"] == "900000"


def test_build_engine_kwargs_sets_tcp_keepalives(monkeypatch):
    monkeypatch.setattr(database.settings, "database_url", "postgresql+asyncpg://u:p@h/d")

    kwargs = database.build_engine_kwargs()

    server_settings = kwargs["connect_args"]["server_settings"]
    assert server_settings["tcp_keepalives_idle"] == "30"
    assert server_settings["tcp_keepalives_interval"] == "10"
    assert server_settings["tcp_keepalives_count"] == "3"


def test_build_engine_kwargs_no_connect_args_for_non_asyncpg(monkeypatch):
    """idle_in_transaction + keepalives must not leak into non-asyncpg URLs."""
    monkeypatch.setattr(database.settings, "database_url", "sqlite+aiosqlite:///:memory:")

    kwargs = database.build_engine_kwargs()

    assert "connect_args" not in kwargs


def test_api_deps_reexport_database_get_db():
    assert deps.get_db is database.get_db
