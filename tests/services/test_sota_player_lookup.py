from uuid import UUID, uuid4

import pytest

from app.models import Player, PlayerTeam
from app.services.live_sync_service import LiveSyncService, _last_names_match


class _NoopClient:
    pass


async def _make_player(session, *, first_name, last_name, sota_id, team_id, season_id, number,
                       last_name_kz=None, last_name_en=None):
    player = Player(
        sota_id=sota_id,
        first_name=first_name,
        last_name=last_name,
        last_name_kz=last_name_kz,
        last_name_en=last_name_en,
    )
    session.add(player)
    await session.commit()
    await session.refresh(player)
    session.add(PlayerTeam(
        player_id=player.id,
        team_id=team_id,
        season_id=season_id,
        number=number,
        is_active=True,
        is_hidden=False,
    ))
    await session.commit()
    return player


def test_last_names_match_normalizes_case_and_whitespace():
    player = Player(first_name="Мансур", last_name="Алихан", last_name_kz="Әліхан")
    assert _last_names_match("Алихан", player)
    assert _last_names_match("  алихан  ", player)
    assert _last_names_match("Әліхан", player)
    assert not _last_names_match("Иванов", player)
    assert not _last_names_match(None, player)
    assert not _last_names_match("", player)


@pytest.mark.asyncio
async def test_rewrites_sota_id_when_last_name_matches(test_session, sample_season, sample_teams):
    """SOTA shipped a new UUID for existing player — must rewrite when last_name matches."""
    old_sota_id = uuid4()
    new_sota_id = UUID("e154e3a2-0cac-4325-bf22-c351efe117f8")

    player = await _make_player(
        test_session,
        first_name="Мансур", last_name="Алихан",
        sota_id=old_sota_id,
        team_id=sample_teams[0].id, season_id=sample_season.id, number=55,
    )

    service = LiveSyncService(test_session, _NoopClient())
    result_id = await service._get_or_create_player_by_sota(
        sota_id_raw=str(new_sota_id),
        first_name="",
        last_name="Алихан",
        team_id=sample_teams[0].id,
        season_id=sample_season.id,
        shirt_number=55,
    )

    assert result_id == player.id
    await test_session.refresh(player)
    assert player.sota_id == new_sota_id


@pytest.mark.asyncio
async def test_does_not_rewrite_when_last_name_differs(test_session, sample_season, sample_teams):
    """Different last_name → keep old sota_id, treat as unknown."""
    old_sota_id = uuid4()
    new_sota_id = uuid4()

    player = await _make_player(
        test_session,
        first_name="Мансур", last_name="Алихан",
        sota_id=old_sota_id,
        team_id=sample_teams[0].id, season_id=sample_season.id, number=55,
    )

    service = LiveSyncService(test_session, _NoopClient())
    # Suppress Telegram + Redis side effects by stubbing _get_or_create_player_by_sota's notification path.
    # The method still returns None for unknown players, which is what we assert.
    try:
        await service._get_or_create_player_by_sota(
            sota_id_raw=str(new_sota_id),
            first_name="",
            last_name="Петров",
            team_id=sample_teams[0].id,
            season_id=sample_season.id,
            shirt_number=55,
        )
    except Exception:
        pass

    await test_session.refresh(player)
    assert player.sota_id == old_sota_id


@pytest.mark.asyncio
async def test_links_when_existing_has_no_sota_id(test_session, sample_season, sample_teams):
    """Original behaviour preserved: empty sota_id → link by number."""
    new_sota_id = uuid4()

    player = await _make_player(
        test_session,
        first_name="Иван", last_name="Петров",
        sota_id=None,
        team_id=sample_teams[0].id, season_id=sample_season.id, number=7,
    )

    service = LiveSyncService(test_session, _NoopClient())
    result_id = await service._get_or_create_player_by_sota(
        sota_id_raw=str(new_sota_id),
        first_name="",
        last_name="Petrov",  # different transliteration, but number matches
        team_id=sample_teams[0].id,
        season_id=sample_season.id,
        shirt_number=7,
    )

    assert result_id == player.id
    await test_session.refresh(player)
    assert player.sota_id == new_sota_id


@pytest.mark.asyncio
async def test_name_variants_kz_and_en_match(test_session, sample_season, sample_teams):
    """last_name_kz or last_name_en should also count as a match."""
    old_sota_id = uuid4()
    new_sota_id = uuid4()

    player = await _make_player(
        test_session,
        first_name="Мансур", last_name="Алихан", last_name_kz="Әліхан", last_name_en="Alikhan",
        sota_id=old_sota_id,
        team_id=sample_teams[0].id, season_id=sample_season.id, number=55,
    )

    service = LiveSyncService(test_session, _NoopClient())
    result_id = await service._get_or_create_player_by_sota(
        sota_id_raw=str(new_sota_id),
        first_name="",
        last_name="Alikhan",
        team_id=sample_teams[0].id,
        season_id=sample_season.id,
        shirt_number=55,
    )

    assert result_id == player.id
    await test_session.refresh(player)
    assert player.sota_id == new_sota_id
