"""Tests for HTTP/DB-transaction decoupling in the player stats sync.

These exercise the inner ``_sync_*_locked`` methods directly (bypassing the
redis outer-mutex wrapper) and assert the core invariants of the collect-then-
write refactor:
  * no transaction is open while SOTA HTTP calls run (no idle-in-transaction);
  * writes happen in chunks, each under its own short transaction;
  * dead-pair state survives chunk boundaries;
  * an all-empty pass opens no write transaction / advisory lock.
"""
from types import SimpleNamespace

import pytest

from app.services.sync.game_sync import GameSyncService
from app.services.sync.guardrails import dead_season_cache_key
from app.services.sync.player_sync import PlayerSyncService
from app.services.sync.player_tour_stats_sync import PlayerTourStatsSyncService
from app.services.sync.stats_sync import StatsSyncService


class FakeFetchAllResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return list(self._rows)


class FakeOneOrNoneResult:
    def __init__(self, row):
        self._row = row

    def one_or_none(self):
        return self._row


class FakeRedis:
    def __init__(self):
        self.keys: set[str] = set()

    async def exists(self, key: str) -> int:
        return int(key in self.keys)

    async def set(self, key: str, value: str, ex: int | None = None) -> bool:
        self.keys.add(key)
        return True


class SpyDB:
    """Records an ordered op log and tracks whether a transaction is open.

    `in_tx` flips True on the first execute after a commit (autobegin) and back
    to False on commit — mirroring SQLAlchemy's commit-as-you-go connection
    lifecycle closely enough to assert "no tx during fetch".
    """

    def __init__(self, player_rows, sota_row):
        self.player_rows = player_rows
        self.sota_row = sota_row
        self.in_tx = False
        self.ops: list[str] = []
        self._data_execute = 0

    async def execute(self, stmt, params=None):
        text_repr = str(getattr(stmt, "text", ""))
        if "pg_advisory_xact_lock" in text_repr:
            self.in_tx = True
            self.ops.append("lock")
            return SimpleNamespace()
        if "lock_timeout" in text_repr:
            self.in_tx = True
            self.ops.append("set_local")
            return SimpleNamespace()
        self.in_tx = True
        self._data_execute += 1
        if self._data_execute == 1:
            self.ops.append("select_players")
            return FakeFetchAllResult(self.player_rows)
        if self._data_execute == 2:
            self.ops.append("select_sota")
            return FakeOneOrNoneResult(self.sota_row)
        self.ops.append("upsert")
        return SimpleNamespace()

    def begin_nested(self):
        class _Ctx:
            async def __aenter__(self_inner):
                return self_inner

            async def __aexit__(self_inner, *exc):
                return False

        return _Ctx()

    async def commit(self):
        self.in_tx = False
        self.ops.append("commit")


class SpySeasonClient:
    """Records whether the DB was in a transaction at each HTTP call."""

    def __init__(self, db, dead_season_id=None):
        self.db = db
        self.dead_season_id = dead_season_id
        self.in_tx_at_call: list[bool] = []
        self.calls: list[tuple[str, int]] = []

    async def get_player_season_stats(self, player_id: str, season_id: int, language: str = "ru"):
        self.in_tx_at_call.append(self.db.in_tx)
        self.calls.append((player_id, season_id))
        if season_id == self.dead_season_id:
            import httpx

            request = httpx.Request("GET", "https://sota.test")
            raise httpx.HTTPStatusError(
                "not found", request=request, response=httpx.Response(404, request=request)
            )
        return {"games_played": 3, "goal": 1}


@pytest.fixture
def _fake_redis(monkeypatch):
    redis = FakeRedis()

    async def fake_get_redis():
        return redis

    monkeypatch.setattr("app.utils.live_flag.get_redis", fake_get_redis)
    return redis


@pytest.mark.asyncio
async def test_no_transaction_open_during_http_fetch(_fake_redis):
    player_rows = [(idx, 10 + idx, f"player-{idx}") for idx in range(1, 6)]
    db = SpyDB(player_rows, ("174", None))
    client = SpySeasonClient(db)
    service = PlayerSyncService(db, client)

    count = await service._sync_player_season_stats_locked(200)

    assert count == 5
    # Every SOTA call happened with no transaction open.
    assert client.in_tx_at_call and not any(client.in_tx_at_call)
    # Reads were committed before the write phase took the advisory lock.
    assert db.ops.index("commit") < db.ops.index("lock")


@pytest.mark.asyncio
async def test_writes_are_chunked_across_500(_fake_redis):
    # 600 players → two write chunks (500 + 100).
    player_rows = [(idx, 10 + idx, f"player-{idx}") for idx in range(1, 601)]
    db = SpyDB(player_rows, ("174", None))
    client = SpySeasonClient(db)
    service = PlayerSyncService(db, client)

    count = await service._sync_player_season_stats_locked(200)

    assert count == 600
    # Two chunks → two advisory-lock acquisitions and three commits
    # (phase-A read commit + one per chunk).
    assert db.ops.count("lock") == 2
    assert db.ops.count("commit") == 3


@pytest.mark.asyncio
async def test_dead_pair_state_persists_across_chunk_boundary(_fake_redis):
    # 600 players, season 173 always 404 → marked dead within the first chunk,
    # then skipped for the rest (including chunk 2). 174 always succeeds.
    player_rows = [(idx, 10 + idx, f"player-{idx}") for idx in range(1, 601)]
    db = SpyDB(player_rows, ("173;174", None))
    client = SpySeasonClient(db, dead_season_id=173)
    service = PlayerSyncService(db, client)

    count = await service._sync_player_season_stats_locked(200)

    assert count == 600
    # 173 stops being called after it is marked dead (min_404 default = 30),
    # well within chunk 1 — so far fewer than 600 calls and none in chunk 2.
    calls_173 = [c for c in client.calls if c[1] == 173]
    calls_174 = [c for c in client.calls if c[1] == 174]
    assert len(calls_173) < 600
    assert len(calls_174) == 600
    assert dead_season_cache_key(200, 173) in _fake_redis.keys


# ---------------------------------------------------------------------------
# Generic queue-based spy for the other sync paths (tour / team / game / best).
# ---------------------------------------------------------------------------

class FakeResult:
    """Supports every accessor the sync code uses on an execute() result."""

    def __init__(self, rows):
        self._rows = list(rows)

    def fetchall(self):
        return list(self._rows)

    def all(self):
        return list(self._rows)

    def one_or_none(self):
        return self._rows[0] if self._rows else None

    def scalar_one_or_none(self):
        return self._rows[0] if self._rows else None

    def scalars(self):
        return _Scalars(self._rows)


class _Scalars:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return list(self._rows)


class QueueSpyDB:
    """Returns queued read results in order; tracks whether a tx is open.

    Distinguishes lock/SET LOCAL (text clauses), writes (Insert/Update) and
    reads (Select) so tests can assert the op ordering and that HTTP runs with
    no transaction open.
    """

    def __init__(self, read_results):
        self.read_results = list(read_results)
        self.in_tx = False
        self.ops: list[str] = []
        self._i = 0

    async def execute(self, stmt, params=None):
        text_repr = str(getattr(stmt, "text", ""))
        if "pg_advisory_xact_lock" in text_repr:
            self.in_tx = True
            self.ops.append("lock")
            return SimpleNamespace()
        if "lock_timeout" in text_repr:
            self.in_tx = True
            self.ops.append("set_local")
            return SimpleNamespace()
        self.in_tx = True
        kind = type(stmt).__name__.lower()
        if "insert" in kind or "update" in kind:
            self.ops.append("write")
            return SimpleNamespace()
        self.ops.append("read")
        if self._i < len(self.read_results):
            result = self.read_results[self._i]
            self._i += 1
            return result
        return FakeResult([])

    def begin_nested(self):
        class _Ctx:
            async def __aenter__(self_inner):
                return self_inner

            async def __aexit__(self_inner, *exc):
                return False

        return _Ctx()

    async def commit(self):
        self.in_tx = False
        self.ops.append("commit")

    async def rollback(self):
        self.in_tx = False
        self.ops.append("rollback")


class InTxRecordingClient:
    """Records db.in_tx at each HTTP call for the given method name."""

    def __init__(self, db, method_name, return_value):
        self.db = db
        self.in_tx_at_call: list[bool] = []
        self._return_value = return_value
        setattr(self, method_name, self._record)

    async def _record(self, *args, **kwargs):
        self.in_tx_at_call.append(self.db.in_tx)
        return self._return_value


@pytest.mark.asyncio
async def test_tour_stats_fetch_sees_no_open_transaction(_fake_redis):
    player_rows = [(1, 11, "p-1"), (2, 12, "p-2")]
    db = QueueSpyDB([FakeResult(player_rows), FakeResult([("174", None)])])
    client = InTxRecordingClient(db, "get_player_game_stats_v2_by_tour", {"games_played": 2, "goal": 1})
    service = PlayerTourStatsSyncService(db, client)

    count = await service._sync_tour_locked(200, 5)

    assert count == 2
    assert client.in_tx_at_call and not any(client.in_tx_at_call)
    assert db.ops.index("commit") < db.ops.index("lock")


@pytest.mark.asyncio
async def test_team_season_stats_fetch_sees_no_open_transaction(monkeypatch):
    # Force the extended (HTTP) path rather than the local-aggregate path.
    monkeypatch.setattr(
        "app.services.sync.stats_sync.get_settings",
        lambda: SimpleNamespace(extended_stats_season_ids={200}),
    )
    score_rows = [SimpleNamespace(team_id=1), SimpleNamespace(team_id=2)]
    db = QueueSpyDB([
        FakeResult(score_rows),     # score_table .scalars().all()
        FakeResult([]),             # participants .scalars().all()
        FakeResult([]),             # games .all()
        FakeResult([("200", None)]),  # get_all_sota_season_ids .one_or_none()
    ])
    client = InTxRecordingClient(db, "get_team_season_stats_v2", {"games_played": 5, "goal": 7})
    service = StatsSyncService(db, client)

    count = await service.sync_team_season_stats(200)

    assert count == 2
    assert client.in_tx_at_call and not any(client.in_tx_at_call)
    # Advisory lock acquired only after the phase-A read commit.
    assert db.ops.index("commit") < db.ops.index("lock")


@pytest.mark.asyncio
async def test_game_v2_enrichment_fetch_sees_no_open_transaction():
    rows = [
        (10, {}, None, None, "sota-1"),
        (11, {}, None, None, "sota-2"),
    ]
    db = QueueSpyDB([FakeResult(rows)])
    client = InTxRecordingClient(db, "get_player_game_stats_v2", {"time_on_field_total": 80})
    service = GameSyncService(db, client)

    enriched = await service._enrich_with_v2_stats(1, "game-uuid")

    assert enriched == 2
    # Every per-player v2 call ran with no transaction open (phase-A commit done).
    assert client.in_tx_at_call and not any(client.in_tx_at_call)
    assert "commit" in db.ops[: db.ops.index("write")]


class OrderRecordingDB(QueueSpyDB):
    """Captures the row id targeted by each UPDATE (from the compiled WHERE bind)."""

    def __init__(self, read_results):
        super().__init__(read_results)
        self.update_ids: list[int] = []

    async def execute(self, stmt, params=None):
        if "update" in type(stmt).__name__.lower():
            compiled = stmt.compile()
            for k, v in compiled.params.items():
                if k.startswith("id"):
                    self.update_ids.append(v)
                    break
        return await super().execute(stmt, params)


@pytest.mark.asyncio
async def test_v2_enrichment_writes_in_ascending_id_order():
    # Rows arrive shuffled; UPDATEs must still go out in ascending id order so
    # two concurrent enrichments of the same game can't deadlock.
    rows = [
        (30, {}, None, None, "sota-30"),
        (10, {}, None, None, "sota-10"),
        (20, {}, None, None, "sota-20"),
    ]
    db = OrderRecordingDB([FakeResult(rows)])
    client = InTxRecordingClient(db, "get_player_game_stats_v2", {"time_on_field_total": 80})
    service = GameSyncService(db, client)

    await service._enrich_with_v2_stats(1, "game-uuid")

    assert db.update_ids == [10, 20, 30]


@pytest.mark.asyncio
async def test_em_enrichment_writes_in_ascending_id_order():
    rows = [(30, {}, "sota-30"), (10, {}, "sota-10"), (20, {}, "sota-20")]
    db = OrderRecordingDB([FakeResult(rows)])

    class EmClient:
        async def get_live_match_player_stats(self, uuid, side):
            if side == "home":
                return [{"id": "sota-30", "shots": 1}, {"id": "sota-10", "shots": 2}]
            return [{"id": "sota-20", "shots": 3}]

    service = GameSyncService(db, EmClient())

    await service._enrich_player_stats_from_em(1, "game-uuid")

    assert db.update_ids == [10, 20, 30]


@pytest.mark.asyncio
async def test_best_players_fetch_sees_no_open_transaction():
    # Phase A reads sota ids then the active player→team lookup, commits, then
    # fetches best-player lists with no transaction open.
    player_teams = [(1, 11, "sota-1"), (2, 12, "sota-2")]
    db = QueueSpyDB([FakeResult([("174", None)]), FakeResult(player_teams)])
    client = InTxRecordingClient(
        db, "get_best_players", [{"id": "sota-1", "value": "5"}, {"id": "sota-2", "value": "3"}]
    )
    service = PlayerSyncService(db, client)

    count = await service.sync_best_players(200)

    assert count == 2
    assert client.in_tx_at_call and not any(client.in_tx_at_call)
    # Lock taken only in the write phase, after the phase-A commit.
    assert db.ops.index("commit") < db.ops.index("lock")


@pytest.mark.asyncio
async def test_no_collected_opens_no_write_transaction(_fake_redis):
    # Client returns no useful stats for anyone → nothing collected.
    class EmptyClient:
        async def get_player_season_stats(self, player_id, season_id, language="ru"):
            return {}

    player_rows = [(idx, 10 + idx, f"player-{idx}") for idx in range(1, 6)]
    db = SpyDB(player_rows, ("174", None))
    service = PlayerSyncService(db, EmptyClient())

    count = await service._sync_player_season_stats_locked(200)

    assert count == 0
    # No advisory lock taken, no upsert, only the phase-A read commit.
    assert "lock" not in db.ops
    assert "upsert" not in db.ops
    assert db.ops.count("commit") == 1
