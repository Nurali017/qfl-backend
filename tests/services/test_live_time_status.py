from datetime import date, time
from uuid import uuid4

import pytest

from app.models import Game, GameEvent, GameEventType, GameStatus
from app.services.live_sync_service import LiveSyncService


class FakeTimeClient:
    def __init__(self, payload):
        self.payload = payload

    async def get_live_match_time(self, _sota_uuid: str):
        return self.payload


@pytest.mark.asyncio
async def test_sync_live_time_sets_halftime_phase(test_session, sample_season, sample_teams):
    game = Game(
        sota_id=uuid4(),
        date=date(2026, 3, 18),
        time=time(18, 0),
        season_id=sample_season.id,
        home_team_id=sample_teams[0].id,
        away_team_id=sample_teams[1].id,
        status=GameStatus.live,
        sync_disabled=True,
    )
    test_session.add(game)
    await test_session.commit()
    await test_session.refresh(game)

    service = LiveSyncService(
        test_session,
        FakeTimeClient({"half": 1, "actual_time": 47 * 60_000, "status": "halftime"}),
    )
    result = await service.sync_live_time(game.id)
    await test_session.refresh(game)

    assert game.status == GameStatus.live
    assert game.finished_at is None
    assert game.live_half == 1
    assert game.live_minute == 47
    assert game.live_phase == "halftime"
    assert result["live_phase"] == "halftime"


@pytest.mark.asyncio
async def test_sync_live_time_sets_in_progress_without_finishing(test_session, sample_season, sample_teams):
    game = Game(
        sota_id=uuid4(),
        date=date(2026, 3, 18),
        time=time(18, 0),
        season_id=sample_season.id,
        home_team_id=sample_teams[0].id,
        away_team_id=sample_teams[1].id,
        status=GameStatus.live,
        sync_disabled=True,
        live_phase="halftime",
    )
    test_session.add(game)
    await test_session.commit()
    await test_session.refresh(game)

    service = LiveSyncService(
        test_session,
        FakeTimeClient({"half": 2, "actual_time": 48 * 60_000, "status": "in_progress"}),
    )
    result = await service.sync_live_time(game.id)
    await test_session.refresh(game)

    assert game.status == GameStatus.live
    assert game.finished_at is None
    assert game.live_half == 2
    assert game.live_minute == 93
    assert game.live_phase == "in_progress"
    assert result["live_phase"] == "in_progress"


@pytest.mark.asyncio
async def test_sync_live_time_finished_uses_lifecycle_service(test_session, sample_season, sample_teams):
    game = Game(
        sota_id=uuid4(),
        date=date(2026, 3, 18),
        time=time(18, 0),
        season_id=sample_season.id,
        home_team_id=sample_teams[0].id,
        away_team_id=sample_teams[1].id,
        status=GameStatus.live,
        sync_disabled=True,
        live_half=2,
        live_minute=93,
        live_phase="in_progress",
    )
    test_session.add(game)
    await test_session.commit()
    await test_session.refresh(game)

    service = LiveSyncService(
        test_session,
        FakeTimeClient({"half": 2, "actual_time": 48 * 60_000, "status": "finished"}),
    )
    result = await service.sync_live_time(game.id)
    await test_session.refresh(game)

    assert game.status == GameStatus.finished
    assert game.finished_at is not None
    assert game.live_half is None
    assert game.live_minute is None
    assert game.live_phase is None
    assert result["lifecycle_result"]["action"] == "finish_live"


@pytest.mark.asyncio
async def test_sync_live_time_et1_offsets_minute_by_90(test_session, sample_season, sample_teams):
    game = Game(
        sota_id=uuid4(),
        date=date(2026, 3, 18),
        time=time(18, 0),
        season_id=sample_season.id,
        home_team_id=sample_teams[0].id,
        away_team_id=sample_teams[1].id,
        status=GameStatus.live,
        sync_disabled=True,
    )
    test_session.add(game)
    await test_session.commit()
    await test_session.refresh(game)

    service = LiveSyncService(
        test_session,
        FakeTimeClient({"half": 3, "actual_time": 5 * 60_000, "status": "in_progress"}),
    )
    await service.sync_live_time(game.id)
    await test_session.refresh(game)

    assert game.live_half == 3
    assert game.live_minute == 95
    assert game.live_phase == "in_progress"


@pytest.mark.asyncio
async def test_sync_live_time_et2_offsets_minute_by_105(test_session, sample_season, sample_teams):
    game = Game(
        sota_id=uuid4(),
        date=date(2026, 3, 18),
        time=time(18, 0),
        season_id=sample_season.id,
        home_team_id=sample_teams[0].id,
        away_team_id=sample_teams[1].id,
        status=GameStatus.live,
        sync_disabled=True,
    )
    test_session.add(game)
    await test_session.commit()
    await test_session.refresh(game)

    service = LiveSyncService(
        test_session,
        FakeTimeClient({"half": 4, "actual_time": 8 * 60_000, "status": "in_progress"}),
    )
    await service.sync_live_time(game.id)
    await test_session.refresh(game)

    assert game.live_half == 4
    assert game.live_minute == 113
    assert game.live_phase == "in_progress"


@pytest.mark.asyncio
async def test_sync_live_time_shootout_uses_round_number_without_offset(
    test_session, sample_season, sample_teams
):
    game = Game(
        sota_id=uuid4(),
        date=date(2026, 3, 18),
        time=time(18, 0),
        season_id=sample_season.id,
        home_team_id=sample_teams[0].id,
        away_team_id=sample_teams[1].id,
        status=GameStatus.live,
        sync_disabled=True,
    )
    test_session.add(game)
    await test_session.commit()
    await test_session.refresh(game)

    service = LiveSyncService(
        test_session,
        FakeTimeClient({"half": 5, "actual_time": 3 * 60_000, "status": "in_progress"}),
    )
    await service.sync_live_time(game.id)
    await test_session.refresh(game)

    assert game.live_half == 5
    assert game.live_minute == 3
    assert game.live_phase == "in_progress"


@pytest.mark.asyncio
async def test_sync_live_time_without_status_keeps_no_artificial_halftime(
    test_session, sample_season, sample_teams
):
    game = Game(
        sota_id=uuid4(),
        date=date(2026, 3, 18),
        time=time(18, 0),
        season_id=sample_season.id,
        home_team_id=sample_teams[0].id,
        away_team_id=sample_teams[1].id,
        status=GameStatus.live,
        sync_disabled=True,
    )
    test_session.add(game)
    await test_session.commit()
    await test_session.refresh(game)

    service = LiveSyncService(
        test_session,
        FakeTimeClient({"half": 2, "actual_time": 48 * 60_000}),
    )
    result = await service.sync_live_time(game.id)
    await test_session.refresh(game)

    assert game.status == GameStatus.live
    assert game.live_half == 2
    assert game.live_minute == 93
    assert game.live_phase is None
    assert result["live_phase"] is None


def _add_shootout_event(test_session, game_id, team_id, scored: bool, minute: int):
    test_session.add(
        GameEvent(
            game_id=game_id,
            half=5,
            minute=minute,
            event_type=GameEventType.penalty if scored else GameEventType.missed_penalty,
            team_id=team_id,
            source="sota",
        )
    )


@pytest.mark.asyncio
async def test_sync_live_time_finished_with_undecided_shootout_keeps_game_live(
    test_session, sample_season, sample_teams
):
    """SOTA marks status=finished at end of ET, but undecided shootout must
    keep the game live with live_phase='shootout'."""
    home_id, away_id = sample_teams[0].id, sample_teams[1].id
    game = Game(
        sota_id=uuid4(),
        date=date(2026, 3, 18),
        time=time(18, 0),
        season_id=sample_season.id,
        home_team_id=home_id,
        away_team_id=away_id,
        status=GameStatus.live,
        sync_disabled=True,
        live_half=4,
        live_minute=120,
        live_phase="in_progress",
        home_score=1,
        away_score=1,
    )
    test_session.add(game)
    await test_session.commit()
    await test_session.refresh(game)

    # 3 successful kicks each, 1 miss each — tied after 4 rounds: undecided
    for minute, team, scored in [
        (1, away_id, True),
        (1, home_id, True),
        (2, away_id, True),
        (2, home_id, False),
        (3, away_id, False),
        (3, home_id, True),
        (4, away_id, True),
        (4, home_id, True),
    ]:
        _add_shootout_event(test_session, game.id, team, scored, minute)
    await test_session.commit()

    service = LiveSyncService(
        test_session,
        FakeTimeClient({"half": 4, "actual_time": 15 * 60_000, "status": "finished"}),
    )
    result = await service.sync_live_time(game.id)
    await test_session.refresh(game)

    assert game.status == GameStatus.live
    assert game.finished_at is None
    assert game.live_half == 5
    assert game.live_phase == "shootout"
    assert game.home_penalty_score == 3
    assert game.away_penalty_score == 3
    assert result.get("shootout_in_progress") is True


@pytest.mark.asyncio
async def test_sync_live_time_finished_with_decided_shootout_finishes(
    test_session, sample_season, sample_teams
):
    """When the shootout has produced an undisputable winner, status=finished
    from SOTA must transition the game to finished as usual."""
    home_id, away_id = sample_teams[0].id, sample_teams[1].id
    game = Game(
        sota_id=uuid4(),
        date=date(2026, 3, 18),
        time=time(18, 0),
        season_id=sample_season.id,
        home_team_id=home_id,
        away_team_id=away_id,
        status=GameStatus.live,
        sync_disabled=True,
        live_half=5,
        live_phase="shootout",
        home_score=1,
        away_score=1,
    )
    test_session.add(game)
    await test_session.commit()
    await test_session.refresh(game)

    # 5 rounds: away 4-1 home. With 0 remaining attempts and lead > remaining → decided.
    for minute, team, scored in [
        (1, away_id, True), (1, home_id, False),
        (2, away_id, True), (2, home_id, False),
        (3, away_id, False), (3, home_id, True),
        (4, away_id, True), (4, home_id, False),
        (5, away_id, True),
    ]:
        _add_shootout_event(test_session, game.id, team, scored, minute)
    await test_session.commit()

    service = LiveSyncService(
        test_session,
        FakeTimeClient({"half": 5, "actual_time": 5 * 60_000, "status": "finished"}),
    )
    result = await service.sync_live_time(game.id)
    await test_session.refresh(game)

    assert game.status == GameStatus.finished
    assert game.finished_at is not None
    assert result["lifecycle_result"]["action"] == "finish_live"


@pytest.mark.asyncio
async def test_recompute_shootout_score_sets_live_phase_and_scores(
    test_session, sample_season, sample_teams
):
    home_id, away_id = sample_teams[0].id, sample_teams[1].id
    game = Game(
        sota_id=uuid4(),
        date=date(2026, 3, 18),
        time=time(18, 0),
        season_id=sample_season.id,
        home_team_id=home_id,
        away_team_id=away_id,
        status=GameStatus.live,
        sync_disabled=True,
    )
    test_session.add(game)
    await test_session.commit()
    await test_session.refresh(game)

    for minute, team, scored in [
        (1, away_id, True),
        (1, home_id, True),
        (2, away_id, True),
        (2, home_id, False),
    ]:
        _add_shootout_event(test_session, game.id, team, scored, minute)
    await test_session.commit()

    service = LiveSyncService(test_session, FakeTimeClient({}))
    result = await service.recompute_shootout_score(game.id)
    await test_session.commit()
    await test_session.refresh(game)

    assert result == {"home_penalty_score": 1, "away_penalty_score": 2}
    assert game.home_penalty_score == 1
    assert game.away_penalty_score == 2
    assert game.live_half == 5
    assert game.live_phase == "shootout"


@pytest.mark.asyncio
async def test_recompute_shootout_score_returns_none_without_events(
    test_session, sample_season, sample_teams
):
    game = Game(
        sota_id=uuid4(),
        date=date(2026, 3, 18),
        time=time(18, 0),
        season_id=sample_season.id,
        home_team_id=sample_teams[0].id,
        away_team_id=sample_teams[1].id,
        status=GameStatus.live,
        sync_disabled=True,
    )
    test_session.add(game)
    await test_session.commit()
    await test_session.refresh(game)

    service = LiveSyncService(test_session, FakeTimeClient({}))
    assert await service.recompute_shootout_score(game.id) is None
