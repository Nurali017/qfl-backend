"""seed round 1 broadcasters for 2026 premier league

Revision ID: c1d2e3f4g5h6
Revises: x1y2z3a4b5c6
Create Date: 2026-03-06 18:10:00.000000
"""

from __future__ import annotations

import re

from alembic import op
import sqlalchemy as sa


revision = "c1d2e3f4g5h6"
down_revision = "x1y2z3a4b5c6"
branch_labels = None
depends_on = None

SEASON_ID = 200
ROUND_NUMBER = 1
SITE_URL = "https://kffleague.kz"

_TEAM_TRANSLATION_TABLE = str.maketrans(
    {
        "ё": "е",
        "ә": "а",
        "ғ": "г",
        "қ": "к",
        "ң": "н",
        "ө": "о",
        "ұ": "у",
        "ү": "у",
        "һ": "х",
        "і": "и",
        "й": "и",
    }
)
_WHITESPACE_RE = re.compile(r"\s+")
_PUNCT_RE = re.compile(r"[^\w\s]+", re.UNICODE)

TEAM_TOKEN_ALIASES = {
    "AKTOBE": ("актобе",),
    "ALTAI_OSKEMEN": ("алтай оскемен", "алтай"),
    "ASTANA": ("астана",),
    "ATYRAU": ("атырау",),
    "ELIMAI": ("елимай",),
    "ERTIS": ("ертис", "иртыш", "ertis"),
    "KAIRAT": ("кайрат",),
    "KAISAR": ("кайсар",),
    "KASPIY": ("каспий",),
    "OKZHETPES": ("окжетпес",),
    "ORDABASY": ("ордабасы",),
    "QYZYLJAR": ("qyzyljar", "quzyljar", "кызылжар"),
    "TOBOL": ("тобыл",),
    "ULYTAU": ("улытау",),
    "ZHENIS": ("женис",),
    "ZHETISU": ("жетису", "жетысу"),
}

BROADCASTERS = (
    {
        "name": "Sport+ Qazaqstan",
        "type": "tv",
        "logo_url": f"{SITE_URL}/sponsors/sport-plus.webp",
        "website": "https://www.sportplustv.kz/",
        "sort_order": 10,
        "aliases": (
            "sport+ qazaqstan",
            "sport plus qazaqstan",
            "sport qazaqstan",
            "sport+ qazaqstan hd",
            "sport plus qazaqstan hd",
        ),
    },
    {
        "name": "QazSport",
        "type": "tv",
        "logo_url": f"{SITE_URL}/sponsors/qazsport.webp",
        "website": "https://qazsporttv.kz/",
        "sort_order": 20,
        "aliases": ("qazsport", "qaz sport", "казспорт"),
    },
    {
        "name": "Kinopoisk",
        "type": "tv",
        "logo_url": f"{SITE_URL}/sponsors/kinopoisk.webp",
        "website": "https://www.kinopoisk.ru/",
        "sort_order": 30,
        "aliases": ("kinopoisk", "кинопоиск"),
    },
    {
        "name": "KFF League",
        "type": "youtube",
        "logo_url": f"{SITE_URL}/sponsors/kff-league.webp",
        "website": "https://www.youtube.com/@KFFLEAGUE-2025",
        "sort_order": 40,
        "aliases": ("kff league", "kffleague", "кфф лига"),
    },
)

ROUND_ONE_BROADCASTS = (
    ("OKZHETPES", "ELIMAI", ("Sport+ Qazaqstan", "Kinopoisk", "KFF League")),
    ("ALTAI_OSKEMEN", "KAIRAT", ("Kinopoisk", "KFF League")),
    ("AKTOBE", "TOBOL", ("Kinopoisk", "KFF League")),
    ("ORDABASY", "ERTIS", ("QazSport", "Kinopoisk")),
    ("QYZYLJAR", "KASPIY", ("Kinopoisk", "KFF League")),
    ("ATYRAU", "ULYTAU", ("Kinopoisk", "KFF League")),
    ("KAISAR", "ZHENIS", ("Sport+ Qazaqstan", "Kinopoisk")),
    ("ASTANA", "ZHETISU", ("Sport+ Qazaqstan", "Kinopoisk")),
)


def _normalize_name(value: str | None) -> str:
    if not value:
        return ""

    normalized = value.casefold().translate(_TEAM_TRANSLATION_TABLE)
    normalized = _PUNCT_RE.sub(" ", normalized)
    normalized = _WHITESPACE_RE.sub(" ", normalized).strip()
    return normalized


def _get_participant_names_by_team_id(bind) -> dict[int, set[str]]:
    rows = bind.execute(
        sa.text(
            """
            SELECT t.id, t.name, t.name_kz, t.name_en
            FROM season_participants sp
            JOIN teams t ON t.id = sp.team_id
            WHERE sp.season_id = :season_id
            """
        ),
        {"season_id": SEASON_ID},
    ).mappings().all()

    names_by_team_id: dict[int, set[str]] = {}
    for row in rows:
        team_id = int(row["id"])
        names = names_by_team_id.setdefault(team_id, set())
        for key in ("name", "name_kz", "name_en"):
            normalized = _normalize_name(row[key])
            if normalized:
                names.add(normalized)

    return names_by_team_id


def _resolve_team_id(token: str, names_by_team_id: dict[int, set[str]]) -> int:
    aliases = tuple(
        _normalize_name(alias) for alias in TEAM_TOKEN_ALIASES.get(token, (token,))
    )

    candidates: set[int] = set()
    for team_id, names in names_by_team_id.items():
        if any(any(alias in name for alias in aliases) for name in names):
            candidates.add(team_id)

    if len(candidates) == 1:
        return next(iter(candidates))

    raise ValueError(
        f"Unable to resolve team token '{token}'. Candidates: {sorted(candidates)}"
    )


def _pick_existing_broadcaster_id(
    rows,
    *,
    canonical_name: str,
    aliases: tuple[str, ...],
) -> int | None:
    canonical = _normalize_name(canonical_name)
    accepted = {canonical, *(_normalize_name(alias) for alias in aliases)}
    matches = [row for row in rows if _normalize_name(row["name"]) in accepted]
    if not matches:
        return None

    matches.sort(
        key=lambda row: (0 if _normalize_name(row["name"]) == canonical else 1, row["id"])
    )
    return int(matches[0]["id"])


def _ensure_broadcasters(bind) -> dict[str, int]:
    existing_rows = bind.execute(
        sa.text("SELECT id, name FROM broadcasters ORDER BY id")
    ).mappings().all()

    broadcaster_ids: dict[str, int] = {}
    for config in BROADCASTERS:
        broadcaster_id = _pick_existing_broadcaster_id(
            existing_rows,
            canonical_name=config["name"],
            aliases=config["aliases"],
        )

        if broadcaster_id is None:
            broadcaster_id = bind.execute(
                sa.text(
                    """
                    INSERT INTO broadcasters (
                        name,
                        logo_url,
                        type,
                        website,
                        sort_order,
                        is_active,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        :name,
                        :logo_url,
                        :type,
                        :website,
                        :sort_order,
                        true,
                        NOW(),
                        NOW()
                    )
                    RETURNING id
                    """
                ),
                {
                    "name": config["name"],
                    "logo_url": config["logo_url"],
                    "type": config["type"],
                    "website": config.get("website"),
                    "sort_order": config["sort_order"],
                },
            ).scalar_one()
        else:
            bind.execute(
                sa.text(
                    """
                    UPDATE broadcasters
                    SET name = :name,
                        logo_url = :logo_url,
                        type = :type,
                        website = :website,
                        sort_order = :sort_order,
                        is_active = true,
                        updated_at = NOW()
                    WHERE id = :broadcaster_id
                    """
                ),
                {
                    "broadcaster_id": broadcaster_id,
                    "name": config["name"],
                    "logo_url": config["logo_url"],
                    "type": config["type"],
                    "website": config.get("website"),
                    "sort_order": config["sort_order"],
                },
            )

        broadcaster_ids[config["name"]] = int(broadcaster_id)

    return broadcaster_ids


def _find_round_one_game_id(
    bind,
    *,
    home_team_id: int,
    away_team_id: int,
) -> int:
    game_id = bind.execute(
        sa.text(
            """
            SELECT id
            FROM games
            WHERE season_id = :season_id
              AND tour = :tour
              AND home_team_id = :home_team_id
              AND away_team_id = :away_team_id
            """
        ),
        {
            "season_id": SEASON_ID,
            "tour": ROUND_NUMBER,
            "home_team_id": home_team_id,
            "away_team_id": away_team_id,
        },
    ).scalar_one_or_none()

    if game_id is None:
        raise ValueError(
            f"Unable to find game for teams {home_team_id} vs {away_team_id}"
        )

    return int(game_id)


def _replace_game_broadcasters(
    bind,
    *,
    game_id: int,
    broadcaster_ids: tuple[int, ...],
) -> None:
    bind.execute(
        sa.text("DELETE FROM game_broadcasters WHERE game_id = :game_id"),
        {"game_id": game_id},
    )

    for index, broadcaster_id in enumerate(broadcaster_ids, start=1):
        bind.execute(
            sa.text(
                """
                INSERT INTO game_broadcasters (game_id, broadcaster_id, sort_order)
                VALUES (:game_id, :broadcaster_id, :sort_order)
                """
            ),
            {
                "game_id": game_id,
                "broadcaster_id": broadcaster_id,
                "sort_order": index * 10,
            },
        )


def upgrade() -> None:
    bind = op.get_bind()
    names_by_team_id = _get_participant_names_by_team_id(bind)
    broadcaster_ids = _ensure_broadcasters(bind)

    for home_token, away_token, broadcaster_names in ROUND_ONE_BROADCASTS:
        home_team_id = _resolve_team_id(home_token, names_by_team_id)
        away_team_id = _resolve_team_id(away_token, names_by_team_id)
        game_id = _find_round_one_game_id(
            bind,
            home_team_id=home_team_id,
            away_team_id=away_team_id,
        )
        _replace_game_broadcasters(
            bind,
            game_id=game_id,
            broadcaster_ids=tuple(broadcaster_ids[name] for name in broadcaster_names),
        )


def downgrade() -> None:
    bind = op.get_bind()
    names_by_team_id = _get_participant_names_by_team_id(bind)

    for home_token, away_token, _ in ROUND_ONE_BROADCASTS:
        home_team_id = _resolve_team_id(home_token, names_by_team_id)
        away_team_id = _resolve_team_id(away_token, names_by_team_id)
        game_id = _find_round_one_game_id(
            bind,
            home_team_id=home_team_id,
            away_team_id=away_team_id,
        )
        bind.execute(
            sa.text("DELETE FROM game_broadcasters WHERE game_id = :game_id"),
            {"game_id": game_id},
        )
