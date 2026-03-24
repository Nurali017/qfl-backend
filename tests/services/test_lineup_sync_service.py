"""Tests for LineupSyncService.sync_pre_game_lineup (em-feed-only flow)."""
from __future__ import annotations

from datetime import date, time
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Game, GameLineup, LineupType, Player, Season, Team, Championship


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_feed_entry(
    number: int | str,
    first_name: str = "",
    last_name: str = "",
    player_id: str = "",
    *,
    gk: bool = False,
    amplua: str = "",
    position: str = "",
    capitan: bool = False,
) -> dict:
    return {
        "Number": str(number),
        "First_name": first_name,
        "Last_name": last_name,
        "Full_name": f"{first_name} {last_name}".strip(),
        "id": player_id,
        "Gk": gk,
        "Amplua": amplua,
        "Position": position,
        "Capitan": capitan,
    }


def _build_em_feed(
    starters: list[dict],
    subs: list[dict],
    *,
    formation: str | None = None,
    kit_color: str | None = None,
) -> list[dict]:
    """Build a realistic /em/ feed payload with ОСНОВНЫЕ/ЗАПАСНЫЕ markers."""
    feed: list[dict] = []
    feed.append({"Number": "TEAM", "First_name": "Test FC"})
    if formation:
        feed.append({
            "Number": "FORMATION",
            "First_name": formation,
            "Full_name": kit_color or "",
        })
    feed.append({"Number": "ОСНОВНЫЕ"})
    feed.extend(starters)
    feed.append({"Number": "ЗАПАСНЫЕ"})
    feed.extend(subs)
    return feed


def _make_player_sota_ids(n: int) -> list[str]:
    return [str(uuid4()) for _ in range(n)]


# ---------------------------------------------------------------------------
# Dummy client
# ---------------------------------------------------------------------------

class DummyLineupClient:
    """Minimal client stub that returns /em/ feed data."""

    def __init__(self):
        self.live_feeds: dict[str, list[dict]] = {}  # key: "{game_id}-{side}"
        self.vsporte_feeds: dict[str, list[dict]] = {}

    def set_feed(self, game_id: str, side: str, feed: list[dict]):
        self.live_feeds[f"{game_id}-{side}"] = feed

    async def get_live_team_lineup(self, game_id: str, side: str) -> list[dict]:
        key = f"{game_id}-{side}"
        if key not in self.live_feeds:
            raise Exception(f"No feed for {key}")
        return self.live_feeds[key]

    async def get_vsporte_team_lineup(self, vsporte_id: str, side: str) -> list[dict]:
        key = f"{vsporte_id}-{side}"
        if key not in self.vsporte_feeds:
            raise Exception(f"No vsporte feed for {key}")
        return self.vsporte_feeds[key]

    async def get_pre_game_lineup(self, game_id: str) -> dict:
        raise NotImplementedError("pre_game_lineup should not be called")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
async def db_setup(test_session: AsyncSession):
    """Create minimal DB objects for lineup tests."""
    champ = Championship(id=1, name="Test League")
    season = Season(
        id=100, name="2025", championship_id=1,
        date_start=date(2025, 1, 1), date_end=date(2025, 12, 31),
    )
    home_team = Team(id=10, name="Home FC")
    away_team = Team(id=20, name="Away FC")
    test_session.add_all([champ, season, home_team, away_team])
    await test_session.flush()
    return {"season": season, "home_team": home_team, "away_team": away_team}


@pytest.fixture
async def game_with_players(test_session: AsyncSession, db_setup):
    """Create a game and 30 players (15 per team) with sota_ids."""
    game = Game(
        sota_id=uuid4(),
        date=date(2025, 6, 1),
        time=time(18, 0),
        tour=1,
        season_id=db_setup["season"].id,
        home_team_id=db_setup["home_team"].id,
        away_team_id=db_setup["away_team"].id,
    )
    test_session.add(game)
    await test_session.flush()

    home_players: list[Player] = []
    away_players: list[Player] = []
    for i in range(15):
        p = Player(sota_id=uuid4(), first_name=f"HomeP{i}", last_name=f"Last{i}")
        test_session.add(p)
        home_players.append(p)
    for i in range(15):
        p = Player(sota_id=uuid4(), first_name=f"AwayP{i}", last_name=f"Last{i}")
        test_session.add(p)
        away_players.append(p)
    await test_session.flush()

    return {
        "game": game,
        "home_players": home_players,
        "away_players": away_players,
        **db_setup,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sync_pre_game_lineup_adds_players_from_em_feed(
    test_session: AsyncSession, game_with_players,
):
    """sync_pre_game_lineup should insert players from /em/ feed with correct lineup types."""
    from app.services.sync.lineup_sync import LineupSyncService

    data = game_with_players
    game: Game = data["game"]
    hp = data["home_players"]
    ap = data["away_players"]

    client = DummyLineupClient()

    # Home: 11 starters + 4 subs
    home_starters = [
        _make_feed_entry(i + 1, hp[i].first_name, hp[i].last_name,
                         str(hp[i].sota_id), gk=(i == 0), amplua="GK" if i == 0 else "M",
                         position="C")
        for i in range(11)
    ]
    home_subs = [
        _make_feed_entry(i + 12, hp[i + 11].first_name, hp[i + 11].last_name,
                         str(hp[i + 11].sota_id))
        for i in range(4)
    ]
    client.set_feed(str(game.sota_id), "home", _build_em_feed(
        home_starters, home_subs, formation="4-4-2",
    ))

    # Away: 11 starters + 4 subs
    away_starters = [
        _make_feed_entry(i + 1, ap[i].first_name, ap[i].last_name,
                         str(ap[i].sota_id), gk=(i == 0), amplua="GK" if i == 0 else "D",
                         position="C")
        for i in range(11)
    ]
    away_subs = [
        _make_feed_entry(i + 12, ap[i + 11].first_name, ap[i + 11].last_name,
                         str(ap[i + 11].sota_id))
        for i in range(4)
    ]
    client.set_feed(str(game.sota_id), "away", _build_em_feed(
        away_starters, away_subs, formation="4-3-3",
    ))

    service = LineupSyncService(test_session, client)
    result = await service.sync_pre_game_lineup(game.id)

    assert result["players_added"] > 0
    assert result["lineups"] == result["players_added"]

    # Check lineup rows were created
    rows = (await test_session.execute(
        select(GameLineup).where(GameLineup.game_id == game.id)
    )).scalars().all()
    assert len(rows) == 30  # 15 home + 15 away

    home_rows = [r for r in rows if r.team_id == data["home_team"].id]
    away_rows = [r for r in rows if r.team_id == data["away_team"].id]
    assert len(home_rows) == 15
    assert len(away_rows) == 15

    # Verify starter/sub classification
    home_starters_db = [r for r in home_rows if r.lineup_type == LineupType.starter]
    home_subs_db = [r for r in home_rows if r.lineup_type == LineupType.substitute]
    assert len(home_starters_db) == 11
    assert len(home_subs_db) == 4

    # Verify has_lineup flag updated
    await test_session.refresh(game)
    assert game.has_lineup is True
    assert game.lineup_source == "sota_live"


@pytest.mark.asyncio
async def test_sync_pre_game_lineup_deletes_stale_players(
    test_session: AsyncSession, game_with_players,
):
    """Players not in /em/ feed should be deleted after sync."""
    from app.services.sync.lineup_sync import LineupSyncService

    data = game_with_players
    game: Game = data["game"]
    hp = data["home_players"]
    ap = data["away_players"]

    # Pre-insert a stale player for each team
    stale_home = GameLineup(
        game_id=game.id, team_id=data["home_team"].id,
        player_id=hp[14].id, lineup_type=LineupType.substitute,
        shirt_number=99,
    )
    stale_away = GameLineup(
        game_id=game.id, team_id=data["away_team"].id,
        player_id=ap[14].id, lineup_type=LineupType.substitute,
        shirt_number=99,
    )
    test_session.add_all([stale_home, stale_away])
    await test_session.flush()

    client = DummyLineupClient()

    # Only 11+3=14 players per side (player index 14 is NOT included)
    home_starters = [
        _make_feed_entry(i + 1, hp[i].first_name, hp[i].last_name,
                         str(hp[i].sota_id), gk=(i == 0), amplua="GK" if i == 0 else "M",
                         position="C")
        for i in range(11)
    ]
    home_subs = [
        _make_feed_entry(i + 12, hp[i + 11].first_name, hp[i + 11].last_name,
                         str(hp[i + 11].sota_id))
        for i in range(3)  # only 3 subs, not 4 — player[14] is excluded
    ]
    client.set_feed(str(game.sota_id), "home", _build_em_feed(
        home_starters, home_subs, formation="4-4-2",
    ))

    away_starters = [
        _make_feed_entry(i + 1, ap[i].first_name, ap[i].last_name,
                         str(ap[i].sota_id), gk=(i == 0), amplua="GK" if i == 0 else "D",
                         position="C")
        for i in range(11)
    ]
    away_subs = [
        _make_feed_entry(i + 12, ap[i + 11].first_name, ap[i + 11].last_name,
                         str(ap[i + 11].sota_id))
        for i in range(3)
    ]
    client.set_feed(str(game.sota_id), "away", _build_em_feed(
        away_starters, away_subs, formation="4-3-3",
    ))

    service = LineupSyncService(test_session, client)
    result = await service.sync_pre_game_lineup(game.id)

    assert result["players_deleted"] == 2  # one stale per team

    # Verify stale players no longer in DB
    rows = (await test_session.execute(
        select(GameLineup).where(GameLineup.game_id == game.id)
    )).scalars().all()
    player_ids_in_db = {r.player_id for r in rows}
    assert hp[14].id not in player_ids_in_db
    assert ap[14].id not in player_ids_in_db


@pytest.mark.asyncio
async def test_sync_pre_game_lineup_no_feed_returns_noop(
    test_session: AsyncSession, game_with_players,
):
    """When /em/ feed is unavailable, sync should return zeros gracefully."""
    from app.services.sync.lineup_sync import LineupSyncService

    data = game_with_players
    game: Game = data["game"]

    client = DummyLineupClient()
    # No feeds set → all fetches will raise exceptions

    service = LineupSyncService(test_session, client)
    result = await service.sync_pre_game_lineup(game.id)

    assert result["players_added"] == 0
    assert result["lineups"] == 0
    assert result["players_deleted"] == 0


@pytest.mark.asyncio
async def test_sync_pre_game_lineup_no_sota_id(
    test_session: AsyncSession, db_setup,
):
    """Game without sota_id should return immediately."""
    from app.services.sync.lineup_sync import LineupSyncService

    game = Game(
        date=date(2025, 6, 1), time=time(18, 0), tour=1,
        season_id=db_setup["season"].id,
        home_team_id=db_setup["home_team"].id,
        away_team_id=db_setup["away_team"].id,
    )
    test_session.add(game)
    await test_session.flush()

    client = DummyLineupClient()
    service = LineupSyncService(test_session, client)
    result = await service.sync_pre_game_lineup(game.id)

    assert result == {
        "referees": 0, "coaches": 0, "lineups": 0,
        "players_deleted": 0, "positions_updated": 0,
        "players_added": 0, "formations_updated": 0,
        "kit_colors_updated": 0,
    }


@pytest.mark.asyncio
async def test_get_matchday_player_ids_returns_none_without_markers(
    test_session: AsyncSession, game_with_players,
):
    """_get_matchday_player_ids returns None if feed has no ОСНОВНЫЕ/ЗАПАСНЫЕ."""
    from app.services.sync.lineup_sync import LineupSyncService

    data = game_with_players
    game: Game = data["game"]
    hp = data["home_players"]

    client = DummyLineupClient()
    # Feed without section markers — just raw player entries
    raw_feed = [
        _make_feed_entry(i + 1, hp[i].first_name, hp[i].last_name, str(hp[i].sota_id))
        for i in range(15)
    ]
    client.set_feed(str(game.sota_id), "home", raw_feed)

    service = LineupSyncService(test_session, client)
    result = await service._get_matchday_player_ids(
        sota_uuid=str(game.sota_id), side="home", vsporte_id=None,
    )
    assert result is None


@pytest.mark.asyncio
async def test_get_matchday_player_ids_returns_none_if_fewer_than_11(
    test_session: AsyncSession, game_with_players,
):
    """_get_matchday_player_ids returns None if fewer than 11 players resolved."""
    from app.services.sync.lineup_sync import LineupSyncService

    data = game_with_players
    game: Game = data["game"]
    hp = data["home_players"]

    client = DummyLineupClient()
    # Only 5 starters + 3 subs = 8 players
    starters = [
        _make_feed_entry(i + 1, hp[i].first_name, hp[i].last_name,
                         str(hp[i].sota_id), gk=(i == 0), amplua="GK" if i == 0 else "M",
                         position="C")
        for i in range(5)
    ]
    subs = [
        _make_feed_entry(i + 6, hp[i + 5].first_name, hp[i + 5].last_name,
                         str(hp[i + 5].sota_id))
        for i in range(3)
    ]
    client.set_feed(str(game.sota_id), "home", _build_em_feed(starters, subs))

    service = LineupSyncService(test_session, client)
    result = await service._get_matchday_player_ids(
        sota_uuid=str(game.sota_id), side="home", vsporte_id=None,
    )
    assert result is None


@pytest.mark.asyncio
async def test_get_matchday_player_ids_success(
    test_session: AsyncSession, game_with_players,
):
    """_get_matchday_player_ids returns correct set of player IDs."""
    from app.services.sync.lineup_sync import LineupSyncService

    data = game_with_players
    game: Game = data["game"]
    hp = data["home_players"]

    client = DummyLineupClient()
    starters = [
        _make_feed_entry(i + 1, hp[i].first_name, hp[i].last_name,
                         str(hp[i].sota_id), gk=(i == 0), amplua="GK" if i == 0 else "M",
                         position="C")
        for i in range(11)
    ]
    subs = [
        _make_feed_entry(i + 12, hp[i + 11].first_name, hp[i + 11].last_name,
                         str(hp[i + 11].sota_id))
        for i in range(3)
    ]
    client.set_feed(str(game.sota_id), "home", _build_em_feed(starters, subs))

    service = LineupSyncService(test_session, client)
    result = await service._get_matchday_player_ids(
        sota_uuid=str(game.sota_id), side="home", vsporte_id=None,
    )
    assert result is not None
    assert len(result) == 14
    expected_ids = {hp[i].id for i in range(14)}
    assert result == expected_ids


@pytest.mark.asyncio
async def test_sync_pre_game_lineup_does_not_call_pre_game_lineup(
    test_session: AsyncSession, game_with_players,
):
    """Verify that the old pre_game_lineup endpoint is never called."""
    from app.services.sync.lineup_sync import LineupSyncService

    data = game_with_players
    game: Game = data["game"]
    hp = data["home_players"]
    ap = data["away_players"]

    client = DummyLineupClient()

    # Set up minimal valid feeds
    for side, players in (("home", hp), ("away", ap)):
        starters = [
            _make_feed_entry(i + 1, players[i].first_name, players[i].last_name,
                             str(players[i].sota_id), gk=(i == 0),
                             amplua="GK" if i == 0 else "M", position="C")
            for i in range(11)
        ]
        subs_list = [
            _make_feed_entry(i + 12, players[i + 11].first_name, players[i + 11].last_name,
                             str(players[i + 11].sota_id))
            for i in range(4)
        ]
        client.set_feed(str(game.sota_id), side, _build_em_feed(starters, subs_list))

    service = LineupSyncService(test_session, client)
    # get_pre_game_lineup raises NotImplementedError → if called, test fails
    await service.sync_pre_game_lineup(game.id)


@pytest.mark.asyncio
async def test_sync_pre_game_lineup_sets_formation(
    test_session: AsyncSession, game_with_players,
):
    """Formation from /em/ feed should be set on the game."""
    from app.services.sync.lineup_sync import LineupSyncService

    data = game_with_players
    game: Game = data["game"]
    hp = data["home_players"]
    ap = data["away_players"]

    client = DummyLineupClient()

    for side, players, formation in (("home", hp, "4-4-2"), ("away", ap, "3-5-2")):
        starters = [
            _make_feed_entry(i + 1, players[i].first_name, players[i].last_name,
                             str(players[i].sota_id), gk=(i == 0),
                             amplua="GK" if i == 0 else "M", position="C")
            for i in range(11)
        ]
        subs_list = [
            _make_feed_entry(i + 12, players[i + 11].first_name, players[i + 11].last_name,
                             str(players[i + 11].sota_id))
            for i in range(4)
        ]
        client.set_feed(str(game.sota_id), side, _build_em_feed(
            starters, subs_list, formation=formation,
        ))

    service = LineupSyncService(test_session, client)
    result = await service.sync_pre_game_lineup(game.id)

    assert result["formations_updated"] >= 2

    await test_session.refresh(game)
    assert game.home_formation == "4-4-2"
    assert game.away_formation == "3-5-2"


# ---------------------------------------------------------------------------
# Tests: _ensure_player_exists — backfill & deduplication
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ensure_player_exists_backfills_empty_first_name(
    test_session: AsyncSession, db_setup,
):
    """Player found by sota_id with empty first_name gets backfilled."""
    from app.services.sync.lineup_sync import LineupSyncService

    sota_id = uuid4()
    player = Player(sota_id=sota_id, first_name="", last_name="Петрович")
    test_session.add(player)
    await test_session.flush()

    client = DummyLineupClient()
    service = LineupSyncService(test_session, client)
    result_id = await service._ensure_player_exists({
        "id": str(sota_id),
        "first_name": "Богдан",
        "last_name": "Петрович",
    })

    assert result_id == player.id
    await test_session.refresh(player)
    assert player.first_name == "Богдан"
    assert player.last_name == "Петрович"


@pytest.mark.asyncio
async def test_ensure_player_exists_does_not_overwrite_existing_name(
    test_session: AsyncSession, db_setup,
):
    """Player found by sota_id with existing first_name should NOT be overwritten."""
    from app.services.sync.lineup_sync import LineupSyncService

    sota_id = uuid4()
    player = Player(sota_id=sota_id, first_name="Иван", last_name="Петров")
    test_session.add(player)
    await test_session.flush()

    client = DummyLineupClient()
    service = LineupSyncService(test_session, client)
    result_id = await service._ensure_player_exists({
        "id": str(sota_id),
        "first_name": "Другое",
        "last_name": "Другое",
    })

    assert result_id == player.id
    await test_session.refresh(player)
    assert player.first_name == "Иван"
    assert player.last_name == "Петров"


@pytest.mark.asyncio
async def test_ensure_player_exists_links_sota_id_to_fcms_player(
    test_session: AsyncSession, db_setup,
):
    """Player created by FCMS (no sota_id) should get sota_id linked instead of creating duplicate."""
    from app.services.sync.lineup_sync import LineupSyncService

    # FCMS-created player with no sota_id
    fcms_player = Player(
        first_name="Богдан", last_name="Петрович",
        fcms_person_id=12345,
    )
    test_session.add(fcms_player)
    await test_session.flush()

    new_sota_id = uuid4()
    client = DummyLineupClient()
    service = LineupSyncService(test_session, client)
    result_id = await service._ensure_player_exists({
        "id": str(new_sota_id),
        "first_name": "Богдан",
        "last_name": "Петрович",
    })

    # Should link to existing player, NOT create a new one
    assert result_id == fcms_player.id
    await test_session.refresh(fcms_player)
    assert fcms_player.sota_id == new_sota_id

    # Verify no duplicate was created
    all_players = (await test_session.execute(
        select(Player).where(Player.last_name == "Петрович")
    )).scalars().all()
    assert len(all_players) == 1


@pytest.mark.asyncio
async def test_ensure_player_exists_ambiguous_name_creates_new(
    test_session: AsyncSession, db_setup,
):
    """When multiple players share the same name, create a new one (safe fallback)."""
    from app.services.sync.lineup_sync import LineupSyncService

    # Two players with same name, no sota_id
    p1 = Player(first_name="Алексей", last_name="Иванов")
    p2 = Player(first_name="Алексей", last_name="Иванов")
    test_session.add_all([p1, p2])
    await test_session.flush()

    new_sota_id = uuid4()
    client = DummyLineupClient()
    service = LineupSyncService(test_session, client)
    result_id = await service._ensure_player_exists({
        "id": str(new_sota_id),
        "first_name": "Алексей",
        "last_name": "Иванов",
    })

    # Should create a new player (ambiguous match)
    assert result_id is not None
    assert result_id != p1.id
    assert result_id != p2.id
