"""Set exact times for rounds 1-3 of 2026 PL and clear tentative flag there.

Revision ID: u3v4w5x6y7z8
Revises: t1u2v3w4x5y6
Create Date: 2026-02-24 00:35:00.000000
"""

from __future__ import annotations

from datetime import date, time
import re

from alembic import op
import sqlalchemy as sa


revision = "u3v4w5x6y7z8"
down_revision = "t1u2v3w4x5y6"
branch_labels = None
depends_on = None

SEASON_ID = 200
SUPER_CUP_DATE = date(2026, 2, 28)
SUPER_CUP_TIME = time(17, 0)
SUPER_CUP_HOME_TOKEN = "KAIRAT"
SUPER_CUP_AWAY_TOKEN = "TOBOL"

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
    "TOBOL": ("тобыл",),
    "ALTAI_OSKEMEN": ("алтай оскемен", "алтай"),
    "ASTANA": ("астана",),
    "ATYRAU": ("атырау",),
    "ULYTAU": ("улытау",),
    "KAISAR": ("кайсар",),
    "ZHENIS": ("женис",),
    "QYZYLJAR": ("qyzyljar", "quzyljar", "кызылжар"),
    "KASPIY": ("каспий",),
    "OKZHETPES": ("окжетпес",),
    "ELIMAI": ("елимай",),
    "ORDABASY": ("ордабасы",),
    "ERTIS": ("ертис", "иртыш", "ertis"),
    "KAIRAT": ("кайрат",),
    "ZHETISU": ("жетису", "жетысу"),
}

ROUND_1_3_SCHEDULE = [
    (1, date(2026, 3, 7), "OKZHETPES", "ELIMAI", time(14, 0)),
    (1, date(2026, 3, 7), "ORDABASY", "ERTIS", time(15, 0)),
    (1, date(2026, 3, 7), "ALTAI_OSKEMEN", "KAIRAT", time(16, 0)),
    (1, date(2026, 3, 7), "AKTOBE", "TOBOL", time(17, 0)),
    (1, date(2026, 3, 8), "QYZYLJAR", "KASPIY", time(14, 0)),
    (1, date(2026, 3, 8), "ATYRAU", "ULYTAU", time(15, 0)),
    (1, date(2026, 3, 8), "KAISAR", "ZHENIS", time(16, 0)),
    (1, date(2026, 3, 8), "ASTANA", "ZHETISU", time(18, 0)),
    (2, date(2026, 3, 14), "KAISAR", "ASTANA", time(15, 0)),
    (2, date(2026, 3, 14), "ELIMAI", "ALTAI_OSKEMEN", time(16, 0)),
    (2, date(2026, 3, 14), "KAIRAT", "AKTOBE", time(17, 0)),
    (2, date(2026, 3, 14), "TOBOL", "ZHETISU", time(18, 0)),
    (2, date(2026, 3, 15), "ERTIS", "ATYRAU", time(14, 0)),
    (2, date(2026, 3, 15), "KASPIY", "OKZHETPES", time(15, 0)),
    (2, date(2026, 3, 15), "ULYTAU", "QYZYLJAR", time(16, 0)),
    (2, date(2026, 3, 15), "ZHENIS", "ORDABASY", time(19, 0)),
    (3, date(2026, 3, 19), "OKZHETPES", "ULYTAU", time(14, 0)),
    (3, date(2026, 3, 19), "ORDABASY", "KAISAR", time(15, 0)),
    (3, date(2026, 3, 19), "ZHETISU", "KAIRAT", time(16, 0)),
    (3, date(2026, 3, 19), "AKTOBE", "ELIMAI", time(17, 0)),
    (3, date(2026, 3, 20), "QYZYLJAR", "ERTIS", time(15, 0)),
    (3, date(2026, 3, 20), "ALTAI_OSKEMEN", "KASPIY", time(16, 0)),
    (3, date(2026, 3, 20), "ATYRAU", "ZHENIS", time(17, 0)),
    (3, date(2026, 3, 20), "ASTANA", "TOBOL", time(19, 0)),
]


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


def _update_match(
    bind,
    *,
    tour: int,
    game_date: date,
    game_time: time,
    home_team_id: int,
    away_team_id: int,
) -> None:
    result = bind.execute(
        sa.text(
            """
            UPDATE games
            SET date = :game_date,
                time = :game_time,
                is_schedule_tentative = false,
                updated_at = NOW()
            WHERE season_id = :season_id
              AND tour = :tour
              AND home_team_id = :home_team_id
              AND away_team_id = :away_team_id
            """
        ),
        {
            "season_id": SEASON_ID,
            "tour": tour,
            "game_date": game_date,
            "game_time": game_time,
            "home_team_id": home_team_id,
            "away_team_id": away_team_id,
        },
    )

    if result.rowcount and result.rowcount > 0:
        return

    reversed_result = bind.execute(
        sa.text(
            """
            UPDATE games
            SET home_team_id = :home_team_id,
                away_team_id = :away_team_id,
                date = :game_date,
                time = :game_time,
                is_schedule_tentative = false,
                updated_at = NOW()
            WHERE season_id = :season_id
              AND tour = :tour
              AND home_team_id = :away_team_id
              AND away_team_id = :home_team_id
            """
        ),
        {
            "season_id": SEASON_ID,
            "tour": tour,
            "game_date": game_date,
            "game_time": game_time,
            "home_team_id": home_team_id,
            "away_team_id": away_team_id,
        },
    )

    if reversed_result.rowcount and reversed_result.rowcount > 0:
        return

    raise ValueError(
        "Unable to find round 1-3 match for update: "
        f"season_id={SEASON_ID}, tour={tour}, "
        f"home_team_id={home_team_id}, away_team_id={away_team_id}"
    )


def _update_supercup(
    bind, *, home_team_id: int, away_team_id: int, game_date: date, game_time: time
) -> None:
    result = bind.execute(
        sa.text(
            """
            UPDATE games
            SET date = :game_date,
                time = :game_time,
                is_schedule_tentative = false,
                updated_at = NOW()
            WHERE season_id = :season_id
              AND tour IS NULL
              AND date = :game_date
              AND home_team_id = :home_team_id
              AND away_team_id = :away_team_id
            """
        ),
        {
            "season_id": SEASON_ID,
            "game_date": game_date,
            "game_time": game_time,
            "home_team_id": home_team_id,
            "away_team_id": away_team_id,
        },
    )

    if result.rowcount and result.rowcount > 0:
        return

    reversed_result = bind.execute(
        sa.text(
            """
            UPDATE games
            SET home_team_id = :home_team_id,
                away_team_id = :away_team_id,
                date = :game_date,
                time = :game_time,
                is_schedule_tentative = false,
                updated_at = NOW()
            WHERE season_id = :season_id
              AND tour IS NULL
              AND date = :game_date
              AND home_team_id = :away_team_id
              AND away_team_id = :home_team_id
            """
        ),
        {
            "season_id": SEASON_ID,
            "game_date": game_date,
            "game_time": game_time,
            "home_team_id": home_team_id,
            "away_team_id": away_team_id,
        },
    )

    if reversed_result.rowcount and reversed_result.rowcount > 0:
        return

    raise ValueError(
        "Unable to find SUPERCUP match for update: "
        f"season_id={SEASON_ID}, date={game_date}, "
        f"home_team_id={home_team_id}, away_team_id={away_team_id}"
    )


def upgrade() -> None:
    bind = op.get_bind()
    names_by_team_id = _get_participant_names_by_team_id(bind)

    token_to_team_id = {
        token: _resolve_team_id(token, names_by_team_id) for token in TEAM_TOKEN_ALIASES
    }

    for tour, game_date, home_token, away_token, game_time in ROUND_1_3_SCHEDULE:
        _update_match(
            bind,
            tour=tour,
            game_date=game_date,
            game_time=game_time,
            home_team_id=token_to_team_id[home_token],
            away_team_id=token_to_team_id[away_token],
        )

    _update_supercup(
        bind,
        home_team_id=token_to_team_id[SUPER_CUP_HOME_TOKEN],
        away_team_id=token_to_team_id[SUPER_CUP_AWAY_TOKEN],
        game_date=SUPER_CUP_DATE,
        game_time=SUPER_CUP_TIME,
    )


def downgrade() -> None:
    bind = op.get_bind()
    bind.execute(
        sa.text(
            """
            UPDATE games
            SET is_schedule_tentative = true,
                updated_at = NOW()
            WHERE season_id = :season_id
              AND (
                  tour BETWEEN 1 AND 3
                  OR (tour IS NULL AND date = :super_cup_date)
              )
            """
        ),
        {"season_id": SEASON_ID, "super_cup_date": SUPER_CUP_DATE},
    )
