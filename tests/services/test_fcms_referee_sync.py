"""Tests for FcmsRefereeSyncService."""
from __future__ import annotations

import logging

import pytest

from app.models import GameReferee, Referee, RefereeRole
from app.services.fcms_referee_sync import (
    _FCMS_REF_ROLE_MAP,
    FcmsRefereeSyncService,
)


# ---------------------------------------------------------------------------
# Pure-function tests
# ---------------------------------------------------------------------------

def test_role_map_covers_all_known_fcms_role_types():
    """Discovered via PL-2026 matches; see FCMS_API.md §4a."""
    expected = {
        "REFEREE",
        "ASSISTANT_REFEREE_1ST",
        "ASSISTANT_REFEREE_2ND",
        "FOURTH_OFFICIAL",
        "VIDEO_ASSISTANT_REFEREE",
        "ASSISTANT_VIDEO_ASSISTANT_REFEREE_1ST",
        "VAR_OPERATOR",
        "MATCH_COMMISSIONER",
        "MATCH_INSPECTOR",
    }
    assert set(_FCMS_REF_ROLE_MAP.keys()) == expected


def test_role_map_targets_are_valid_enum_values():
    for role in _FCMS_REF_ROLE_MAP.values():
        assert isinstance(role, RefereeRole)


# ---------------------------------------------------------------------------
# Fake FCMS client
# ---------------------------------------------------------------------------

class _FakeFcmsClient:
    def __init__(self, payload: list[dict]):
        self.payload = payload
        self.calls: list[int] = []

    async def get_match_official_allocations(self, match_id: int) -> list[dict]:
        self.calls.append(match_id)
        return self.payload


def _alloc(role_type: str, person_id: int, ln_ru: str, fn_ru: str, *, iso2: str = "KZ") -> dict:
    return {
        "matchOfficialRole": {"roleType": role_type, "title": role_type},
        "matchOfficial": {
            "personId": person_id,
            "firstName": fn_ru,
            "familyName": ln_ru,
            "localFirstName": fn_ru,
            "localFamilyName": ln_ru,
            "nationalCitizenships": [{"iso2": iso2}],
        },
        "status": "CONFIRMED",
    }


# ---------------------------------------------------------------------------
# Integration tests (sqlite via test_session fixture)
# ---------------------------------------------------------------------------

@pytest.fixture
async def game_with_fcms(test_session, sample_game):
    sample_game.fcms_match_id = 999001
    await test_session.commit()
    return sample_game


@pytest.mark.asyncio
async def test_sync_creates_missing_referees(test_session, game_with_fcms):
    payload = [
        _alloc("REFEREE", 1001, "Иванов", "Петр"),
        _alloc("ASSISTANT_REFEREE_1ST", 1002, "Сидоров", "Иван"),
    ]
    svc = FcmsRefereeSyncService(test_session, _FakeFcmsClient(payload))

    res = await svc.sync_match_referees(game_with_fcms.id)

    assert res["added"] == 2
    assert res["created_referees"] == 2
    assert res["removed"] == 0
    assert res["updated"] == 0

    # DB state
    refs = (await test_session.execute(
        Referee.__table__.select().order_by(Referee.fcms_person_id)
    )).all()
    assert {r.fcms_person_id for r in refs} == {1001, 1002}

    grs = (await test_session.execute(
        GameReferee.__table__.select().where(GameReferee.game_id == game_with_fcms.id)
    )).all()
    assert {gr.role for gr in grs} == {RefereeRole.main, RefereeRole.first_assistant}


@pytest.mark.asyncio
async def test_sync_reuses_existing_referee_by_fcms_person_id(test_session, game_with_fcms):
    existing = Referee(
        first_name="Петр", last_name="Иванов",
        fcms_person_id=1001,
    )
    test_session.add(existing)
    await test_session.commit()

    payload = [_alloc("REFEREE", 1001, "Иванов", "Петр")]
    svc = FcmsRefereeSyncService(test_session, _FakeFcmsClient(payload))

    res = await svc.sync_match_referees(game_with_fcms.id)
    assert res["added"] == 1
    assert res["created_referees"] == 0


@pytest.mark.asyncio
async def test_sync_idempotent_no_changes_on_second_run(test_session, game_with_fcms):
    payload = [_alloc("REFEREE", 1001, "Иванов", "Петр")]
    svc = FcmsRefereeSyncService(test_session, _FakeFcmsClient(payload))

    res1 = await svc.sync_match_referees(game_with_fcms.id)
    await test_session.commit()
    res2 = await svc.sync_match_referees(game_with_fcms.id)

    assert res1["added"] == 1 and res1["created_referees"] == 1
    assert res2["added"] == 0 and res2["updated"] == 0 and res2["removed"] == 0


@pytest.mark.asyncio
async def test_sync_replaces_referee_when_fcms_changes(test_session, game_with_fcms):
    payload = [_alloc("REFEREE", 1001, "Иванов", "Петр")]
    svc = FcmsRefereeSyncService(test_session, _FakeFcmsClient(payload))
    await svc.sync_match_referees(game_with_fcms.id)
    await test_session.commit()

    new_payload = [_alloc("REFEREE", 1002, "Сидоров", "Иван")]
    svc2 = FcmsRefereeSyncService(test_session, _FakeFcmsClient(new_payload))
    res = await svc2.sync_match_referees(game_with_fcms.id)

    assert res["updated"] == 1
    assert res["added"] == 0
    assert res["removed"] == 0


@pytest.mark.asyncio
async def test_sync_removes_role_dropped_from_fcms(test_session, game_with_fcms):
    payload = [
        _alloc("REFEREE", 1001, "Иванов", "Петр"),
        _alloc("FOURTH_OFFICIAL", 1003, "Кузнецов", "Сергей"),
    ]
    svc = FcmsRefereeSyncService(test_session, _FakeFcmsClient(payload))
    await svc.sync_match_referees(game_with_fcms.id)
    await test_session.commit()

    payload2 = [_alloc("REFEREE", 1001, "Иванов", "Петр")]
    res = await FcmsRefereeSyncService(test_session, _FakeFcmsClient(payload2)).sync_match_referees(
        game_with_fcms.id
    )
    assert res["removed"] == 1


@pytest.mark.asyncio
async def test_sync_skips_unknown_role_type(test_session, game_with_fcms, caplog):
    payload = [
        _alloc("REFEREE", 1001, "Иванов", "Петр"),
        _alloc("UNKNOWN_FUTURE_ROLE", 9999, "Иной", "Имя"),
    ]
    svc = FcmsRefereeSyncService(test_session, _FakeFcmsClient(payload))
    with caplog.at_level(logging.WARNING):
        res = await svc.sync_match_referees(game_with_fcms.id)
    assert res["added"] == 1
    assert res["skipped"] == 1
    assert any("UNKNOWN_FUTURE_ROLE" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_sync_returns_error_for_game_without_fcms_id(test_session, sample_game):
    svc = FcmsRefereeSyncService(test_session, _FakeFcmsClient([]))
    res = await svc.sync_match_referees(sample_game.id)
    assert "error" in res


@pytest.mark.asyncio
async def test_sync_matches_existing_by_name_and_backfills_fcms_id(test_session, game_with_fcms):
    existing = Referee(
        first_name="Петр", last_name="Иванов",
    )
    test_session.add(existing)
    await test_session.commit()

    payload = [_alloc("REFEREE", 1001, "Иванов", "Петр")]
    svc = FcmsRefereeSyncService(test_session, _FakeFcmsClient(payload))
    res = await svc.sync_match_referees(game_with_fcms.id)
    await test_session.flush()

    await test_session.refresh(existing)
    assert existing.fcms_person_id == 1001
    assert res["created_referees"] == 0
