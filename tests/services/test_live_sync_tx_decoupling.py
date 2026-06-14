"""Tests for DB/HTTP transaction decoupling in LiveSyncService.

Background: under 4 simultaneous live matches (incident 2026-06-14) the live-sync
steps held a DB connection idle-in-transaction during the sota.id HTTP roundtrip,
exhausting the web pool (10+20=30) → QueuePool timeouts → ~112 user-facing 500s.

The fix commits the (read-only) transaction right before each sota.id HTTP call so
the pooled connection is free during the roundtrip (expire_on_commit=False keeps
ORM objects usable afterwards).

Group A — decoupling proof (RED before the fix, GREEN after): a fake sota client
records `session.in_transaction()` at the moment it is called and we assert it is
False (no transaction held during HTTP).

Group B — dedup regression guards (must stay GREEN through the refactor): prove the
transaction split does not create duplicate events or drop manual-event protection.
"""

from datetime import date, time
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Game, GameEvent, GameEventType, GameStatus
from app.services.live_sync_service import LiveSyncService


# --- Fake sota clients -------------------------------------------------------

class _TxCapturingEventsClient:
    """Records whether a transaction is open at the moment of the HTTP call."""

    def __init__(self, session: AsyncSession, payload):
        self._session = session
        self._payload = payload
        self.in_transaction_at_call = None

    async def get_live_match_events(self, _sota_uuid: str):
        self.in_transaction_at_call = self._session.in_transaction()
        return self._payload


class _TxCapturingStatsClient:
    def __init__(self, session: AsyncSession, payload):
        self._session = session
        self._payload = payload
        self.in_transaction_at_call = None

    async def get_live_match_stats(self, _sota_uuid: str):
        self.in_transaction_at_call = self._session.in_transaction()
        return self._payload


class _TxCapturingPlayerStatsClient:
    """Records in_transaction on every per-side HTTP call (home, away)."""

    def __init__(self, session: AsyncSession):
        self._session = session
        self.in_transaction_calls: list[bool] = []

    async def get_live_match_player_stats(self, _sota_uuid: str, _side: str):
        self.in_transaction_calls.append(self._session.in_transaction())
        return []


class _FakeEventsClient:
    """Returns a fixed events payload; used by dedup tests."""

    def __init__(self, payload):
        self._payload = payload

    async def get_live_match_events(self, _sota_uuid: str):
        return self._payload


# --- Helpers -----------------------------------------------------------------

def _goal_payload(*, first: str, last: str, half: int = 1, minute: int = 20, team: str = ""):
    return {
        "action": "ГОЛ",
        "half": half,
        "time": minute,
        "first_name1": first,
        "last_name1": last,
        "team1": team,
        "number1": None,
    }


async def _make_live_game(session, season, teams, *, with_teams: bool = True):
    game = Game(
        sota_id=uuid4(),
        date=date(2026, 6, 14),
        time=time(18, 0),
        season_id=season.id,
        home_team_id=teams[0].id if with_teams else None,
        away_team_id=teams[1].id if with_teams else None,
        status=GameStatus.live,
        sync_disabled=True,
    )
    session.add(game)
    await session.commit()
    await session.refresh(game)
    return game


async def _count_sota_goals(session, game_id: int) -> int:
    res = await session.execute(
        select(func.count())
        .select_from(GameEvent)
        .where(
            GameEvent.game_id == game_id,
            GameEvent.source == "sota",
            GameEvent.event_type == GameEventType.goal,
        )
    )
    return res.scalar() or 0


# --- Group A: decoupling proof ----------------------------------------------

@pytest.mark.asyncio
async def test_sync_live_events_releases_transaction_before_http(
    test_session, sample_season, sample_teams
):
    game = await _make_live_game(test_session, sample_season, sample_teams)

    client = _TxCapturingEventsClient(test_session, [])
    service = LiveSyncService(test_session, client)
    await service.sync_live_events(game.id)

    assert client.in_transaction_at_call is False, (
        "sota.id events HTTP must run with no open transaction (connection freed)"
    )


@pytest.mark.asyncio
async def test_sync_live_stats_releases_transaction_before_http(
    test_session, sample_season, sample_teams
):
    # Null team ids → GameTeamStats upsert loop is skipped (the PG-dialect
    # on_conflict upsert can't compile on SQLite); the HTTP capture happens
    # before the loop, which is all this test cares about.
    game = await _make_live_game(test_session, sample_season, sample_teams, with_teams=False)

    client = _TxCapturingStatsClient(test_session, [{"metric": "scores", "home": "0", "away": "0"}])
    service = LiveSyncService(test_session, client)
    await service.sync_live_stats(game.id)

    assert client.in_transaction_at_call is False, (
        "sota.id stats HTTP must run with no open transaction (connection freed)"
    )


@pytest.mark.asyncio
async def test_sync_live_player_stats_releases_transaction_before_each_http(
    test_session, sample_season, sample_teams
):
    game = await _make_live_game(test_session, sample_season, sample_teams)

    client = _TxCapturingPlayerStatsClient(test_session)
    service = LiveSyncService(test_session, client)
    await service.sync_live_player_stats(game.id)

    assert client.in_transaction_calls == [False, False], (
        "each per-side sota.id player-stats HTTP must run with no open transaction; "
        f"got {client.in_transaction_calls}"
    )


# --- Group B: dedup regression guards ---------------------------------------

@pytest.mark.asyncio
async def test_manual_event_protected_from_sota_duplicate(
    test_session, sample_season, sample_teams
):
    """A manually-entered event must not be duplicated by a matching SOTA event,
    even with the read/write transaction split."""
    game = await _make_live_game(test_session, sample_season, sample_teams)

    manual = GameEvent(
        game_id=game.id,
        half=1,
        minute=15,
        event_type=GameEventType.goal,
        team_id=sample_teams[0].id,
        player_name="Иван Иванов",
        source="manual",
    )
    test_session.add(manual)
    await test_session.commit()

    # SOTA reports the same goal one minute off (within MANUAL_MINUTE_TOLERANCE).
    client = _FakeEventsClient([_goal_payload(first="Иван", last="Иванов", minute=16)])
    service = LiveSyncService(test_session, client)
    result = await service.sync_live_events(game.id)

    assert result["added"] == 0
    assert await _count_sota_goals(test_session, game.id) == 0

    manual_count = await test_session.execute(
        select(func.count()).select_from(GameEvent).where(
            GameEvent.game_id == game.id, GameEvent.source == "manual"
        )
    )
    assert manual_count.scalar() == 1


@pytest.mark.asyncio
async def test_sota_event_sync_is_idempotent(test_session, sample_season, sample_teams):
    """Re-running the same SOTA payload (burst / next cycle) matches the existing
    event instead of inserting a duplicate."""
    game = await _make_live_game(test_session, sample_season, sample_teams)

    client = _FakeEventsClient([_goal_payload(first="Пётр", last="Петров", minute=20)])
    service = LiveSyncService(test_session, client)

    first = await service.sync_live_events(game.id)
    assert first["added"] == 1
    assert await _count_sota_goals(test_session, game.id) == 1

    second = await service.sync_live_events(game.id)
    assert second["added"] == 0
    assert await _count_sota_goals(test_session, game.id) == 1


@pytest.mark.asyncio
async def test_sota_dedup_across_separate_sessions(
    test_engine, test_session, sample_season, sample_teams
):
    """Burst simulation: two independent sync sessions (as `_run_live_step` opens
    per step) against the same DB must not produce duplicate events. Session A
    inserts and commits; session B reads the committed state and matches."""
    game = await _make_live_game(test_session, sample_season, sample_teams)

    client = _FakeEventsClient([_goal_payload(first="Сергей", last="Сергеев", minute=33)])
    maker = async_sessionmaker(
        test_engine, class_=AsyncSession, expire_on_commit=False, autoflush=False
    )

    async with maker() as s1:
        r1 = await LiveSyncService(s1, client).sync_live_events(game.id)
    async with maker() as s2:
        r2 = await LiveSyncService(s2, client).sync_live_events(game.id)
        total = await _count_sota_goals(s2, game.id)

    assert r1["added"] == 1
    assert r2["added"] == 0
    assert total == 1
