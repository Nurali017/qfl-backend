"""Tests for app.services.kit_color_sync (apps.kffleague.kz kit-colour import)."""
from __future__ import annotations

import io
from datetime import date, time, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Championship, Game, Season, Team
from app.services.kit_color_sync import (
    AppsMatchKit,
    apply_kit_color,
    extract_hex_from_png,
    is_hex_color,
    match_game,
    normalize_team_name,
    sync_kits_for_matches,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _png(rgba: tuple[int, int, int, int], size: int = 24) -> bytes:
    from PIL import Image

    buf = io.BytesIO()
    Image.new("RGBA", (size, size), rgba).save(buf, "PNG")
    return buf.getvalue()


# ---------------------------------------------------------------------------
# unit: normalize_team_name
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "raw, expected",
    [
        ("«Астана»", "астана"),
        ("  «Кайрат»  ", "кайрат"),
        ("Шахтёр", "шахтер"),                 # ё → е
        ("Кызыл-Жар СК", "кызылжарск"),       # hyphen + space dropped
        ("«Тобол»", "тобыл"),                  # alias тобол → тобыл
        ("ТОБОЛ М", "тобылм"),                 # alias + lowercased
        ("«Turkistan»", "туркестан"),          # latin → cyrillic alias
        ("«Akas»", "акас"),
        ("", ""),
        (None, ""),
    ],
)
def test_normalize_team_name(raw, expected):
    assert normalize_team_name(raw) == expected


def test_is_hex_color():
    assert is_hex_color("#1A2B3C")
    assert is_hex_color("#abcdef")
    assert not is_hex_color("1A2B3C")
    assert not is_hex_color("#12345")
    assert not is_hex_color("red")
    assert not is_hex_color(None)
    assert not is_hex_color("")


# ---------------------------------------------------------------------------
# unit: extract_hex_from_png
# ---------------------------------------------------------------------------

def test_extract_hex_solid_red():
    hx = extract_hex_from_png(_png((200, 30, 30, 255)))
    assert hx is not None and is_hex_color(hx)
    r, g, b = int(hx[1:3], 16), int(hx[3:5], 16), int(hx[5:7], 16)
    assert r > g and r > b  # dominant red channel


def test_extract_hex_solid_blue():
    hx = extract_hex_from_png(_png((20, 60, 200, 255)))
    assert hx is not None and is_hex_color(hx)
    r, g, b = int(hx[1:3], 16), int(hx[3:5], 16), int(hx[5:7], 16)
    assert b > r and b > g  # dominant blue channel


def test_extract_hex_half_transparent_uses_visible_colour():
    """A jersey-like icon: a solid colour on a transparent canvas."""
    from PIL import Image

    img = Image.new("RGBA", (40, 40), (0, 0, 0, 0))
    for x in range(8, 32):
        for y in range(8, 32):
            img.putpixel((x, y), (30, 160, 50, 255))  # green patch
    buf = io.BytesIO()
    img.save(buf, "PNG")
    hx = extract_hex_from_png(buf.getvalue())
    assert hx is not None and is_hex_color(hx)
    r, g, b = int(hx[1:3], 16), int(hx[3:5], 16), int(hx[5:7], 16)
    assert g > r and g > b


def test_extract_hex_fully_transparent_returns_none():
    assert extract_hex_from_png(_png((0, 0, 0, 0))) is None


def test_extract_hex_garbage_returns_none():
    assert extract_hex_from_png(b"not a png at all") is None
    assert extract_hex_from_png(b"") is None
    assert extract_hex_from_png(None) is None


# ---------------------------------------------------------------------------
# unit: apply_kit_color
# ---------------------------------------------------------------------------

def test_apply_kit_color_sets_and_normalises_case():
    game = Game()
    game.home_kit_color = None
    assert apply_kit_color(game, "home", "#aabbcc") is True
    assert game.home_kit_color == "#AABBCC"


def test_apply_kit_color_overwrite_default():
    game = Game()
    game.away_kit_color = "#000000"
    assert apply_kit_color(game, "away", "#FF0000") is True
    assert game.away_kit_color == "#FF0000"


def test_apply_kit_color_no_overwrite_when_set():
    game = Game()
    game.home_kit_color = "#123456"
    assert apply_kit_color(game, "home", "#ABCDEF", allow_overwrite=False) is False
    assert game.home_kit_color == "#123456"


def test_apply_kit_color_fills_empty_even_when_no_overwrite():
    game = Game()
    game.home_kit_color = None
    assert apply_kit_color(game, "home", "#ABCDEF", allow_overwrite=False) is True
    assert game.home_kit_color == "#ABCDEF"


def test_apply_kit_color_rejects_bad_hex_and_none():
    game = Game()
    game.home_kit_color = None
    assert apply_kit_color(game, "home", "red") is False
    assert apply_kit_color(game, "home", None) is False
    assert game.home_kit_color is None


def test_apply_kit_color_noop_when_same():
    game = Game()
    game.home_kit_color = "#ABCDEF"
    assert apply_kit_color(game, "home", "#abcdef") is False


# ---------------------------------------------------------------------------
# integration: match_game / sync_kits_for_matches
# ---------------------------------------------------------------------------

@pytest.fixture
async def kit_setup(test_session: AsyncSession):
    champ = Championship(id=1, name="Test League")
    season = Season(
        id=900, name="2025", championship_id=1,
        date_start=date(2025, 1, 1), date_end=date(2025, 12, 31),
    )
    astana = Team(id=801, name="Астана")
    tobyl = Team(id=802, name="Тобыл")  # apps spells it "Тобол"
    test_session.add_all([champ, season, astana, tobyl])
    await test_session.flush()
    game = Game(
        date=date(2025, 5, 10), time=time(18, 0),
        season_id=900, home_team_id=astana.id, away_team_id=tobyl.id,
    )
    test_session.add(game)
    # commit so the fixture data survives a dry-run rollback inside the SUT
    await test_session.commit()
    return {"season": season, "astana": astana, "tobyl": tobyl, "game": game}


@pytest.mark.asyncio
async def test_match_game_exact(test_session: AsyncSession, kit_setup):
    g = await match_game(
        test_session, match_date=date(2025, 5, 10),
        home_name="«Астана»", away_name="«Тобол»",
    )
    assert g is not None and g.id == kit_setup["game"].id


@pytest.mark.asyncio
async def test_match_game_wrong_teams_returns_none(test_session: AsyncSession, kit_setup):
    assert await match_game(
        test_session, match_date=date(2025, 5, 10),
        home_name="«Кайрат»", away_name="«Тобол»",
    ) is None


@pytest.mark.asyncio
async def test_match_game_swapped_sides_returns_none(test_session: AsyncSession, kit_setup):
    # apps says Тобол(home) vs Астана(away); QFL has Астана(home) vs Тобол(away)
    assert await match_game(
        test_session, match_date=date(2025, 5, 10),
        home_name="«Тобол»", away_name="«Астана»",
    ) is None


@pytest.mark.asyncio
async def test_match_game_plus_minus_one_day(test_session: AsyncSession, kit_setup):
    # apps scheduled date is a day before the actual QFL game date
    g = await match_game(
        test_session, match_date=date(2025, 5, 9),
        home_name="Астана", away_name="Тобол",
    )
    assert g is not None and g.id == kit_setup["game"].id
    g2 = await match_game(
        test_session, match_date=date(2025, 5, 11),
        home_name="Астана", away_name="Тобол",
    )
    assert g2 is not None and g2.id == kit_setup["game"].id


@pytest.mark.asyncio
async def test_match_game_far_date_returns_none(test_session: AsyncSession, kit_setup):
    assert await match_game(
        test_session, match_date=date(2025, 6, 1),
        home_name="Астана", away_name="Тобол",
    ) is None


@pytest.mark.asyncio
async def test_match_game_ambiguous_same_date(test_session: AsyncSession, kit_setup):
    # second game, same teams, same date, different time
    g2 = Game(
        date=date(2025, 5, 10), time=time(20, 30),
        season_id=900, home_team_id=801, away_team_id=802,
    )
    test_session.add(g2)
    await test_session.flush()
    # no time hint → ambiguous → None
    assert await match_game(
        test_session, match_date=date(2025, 5, 10),
        home_name="Астана", away_name="Тобол",
    ) is None
    # time hint disambiguates
    g = await match_game(
        test_session, match_date=date(2025, 5, 10),
        home_name="Астана", away_name="Тобол", match_time=time(20, 30),
    )
    assert g is not None and g.id == g2.id


@pytest.mark.asyncio
async def test_sync_kits_for_matches_applies_colours(test_session: AsyncSession, kit_setup):
    records = [
        AppsMatchKit(
            apps_match_id=42,
            match_date=date(2025, 5, 10),
            match_time=time(18, 0),
            home_name="«Астана»",
            away_name="«Тобол»",
            home_image="home.png",
            away_image="away.png",
        ),
        # an unmatchable fixture
        AppsMatchKit(
            apps_match_id=99,
            match_date=date(2025, 5, 10),
            home_name="«Нет»",
            away_name="«Такой»",
            home_image="home.png",
        ),
    ]
    pngs = {"home.png": _png((200, 30, 30, 255)), "away.png": _png((20, 60, 200, 255))}

    result = await sync_kits_for_matches(
        test_session, records, fetch_png=lambda fn: pngs.get(fn),
    )
    assert result.matches_seen == 2
    assert result.matched_games == 1
    assert len(result.unmatched) == 1 and result.unmatched[0][0] == 99
    assert result.games_updated == 1
    assert result.colors_set == 2

    await test_session.refresh(kit_setup["game"])
    assert is_hex_color(kit_setup["game"].home_kit_color)
    assert is_hex_color(kit_setup["game"].away_kit_color)
    # home jersey was red-dominant
    hx = kit_setup["game"].home_kit_color
    assert int(hx[1:3], 16) > int(hx[3:5], 16)


@pytest.mark.asyncio
async def test_sync_kits_for_matches_dry_run_does_not_persist(test_session: AsyncSession, kit_setup):
    records = [
        AppsMatchKit(
            apps_match_id=42, match_date=date(2025, 5, 10),
            home_name="«Астана»", away_name="«Тобол»",
            home_image="home.png",
        ),
    ]
    pngs = {"home.png": _png((200, 30, 30, 255))}
    result = await sync_kits_for_matches(
        test_session, records, fetch_png=lambda fn: pngs.get(fn), dry_run=True,
    )
    assert result.matched_games == 1 and result.colors_set == 1
    # rolled back → re-fetch from a fresh query shows nothing written
    fresh = await match_game(
        test_session, match_date=date(2025, 5, 10),
        home_name="Астана", away_name="Тобол",
    )
    assert fresh is not None and fresh.home_kit_color is None


@pytest.mark.asyncio
async def test_sync_kits_missing_image_counts(test_session: AsyncSession, kit_setup):
    records = [
        AppsMatchKit(
            apps_match_id=42, match_date=date(2025, 5, 10),
            home_name="«Астана»", away_name="«Тобол»",
            home_image="missing.png",
        ),
    ]
    result = await sync_kits_for_matches(
        test_session, records, fetch_png=lambda fn: None,
    )
    assert result.images_missing == 1
    assert result.matched_games == 1
    assert result.games_updated == 0
    await test_session.refresh(kit_setup["game"])
    assert kit_setup["game"].home_kit_color is None
