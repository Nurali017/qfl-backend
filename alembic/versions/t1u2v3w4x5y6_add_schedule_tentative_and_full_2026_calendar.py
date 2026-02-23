"""Add tentative schedule flag and complete 2026 PL calendar.

Revision ID: t1u2v3w4x5y6
Revises: f9g0h1i2j3k4, p4k5l6m7n8o9
Create Date: 2026-02-24 00:10:00.000000
"""

from __future__ import annotations

from datetime import date
import re

from alembic import op
import sqlalchemy as sa


revision = "t1u2v3w4x5y6"
down_revision = ("f9g0h1i2j3k4", "p4k5l6m7n8o9")
branch_labels = None
depends_on = None

SEASON_ID = 200
SUPER_CUP_DATE = date(2026, 2, 28)
SUPER_CUP_HOME_TOKEN = "ҚАЙРАТ"
SUPER_CUP_AWAY_TOKEN = "ТОБЫЛ"

# Dates use the start date of each tour window.
# Tour 27 is explicitly fixed to 2026-09-17 due the mixed slot in the source PDF.
TOUR_START_DATES = {
    1: date(2026, 3, 7),
    2: date(2026, 3, 14),
    3: date(2026, 3, 19),
    4: date(2026, 4, 4),
    5: date(2026, 4, 11),
    6: date(2026, 4, 16),
    7: date(2026, 4, 25),
    8: date(2026, 5, 2),
    9: date(2026, 5, 9),
    10: date(2026, 5, 16),
    11: date(2026, 5, 22),
    12: date(2026, 5, 27),
    13: date(2026, 6, 13),
    14: date(2026, 6, 20),
    15: date(2026, 6, 27),
    16: date(2026, 7, 4),
    17: date(2026, 7, 11),
    18: date(2026, 7, 18),
    19: date(2026, 7, 25),
    20: date(2026, 8, 1),
    21: date(2026, 8, 8),
    22: date(2026, 8, 15),
    23: date(2026, 8, 22),
    24: date(2026, 8, 29),
    25: date(2026, 9, 5),
    26: date(2026, 9, 12),
    27: date(2026, 9, 17),
    28: date(2026, 10, 10),
    29: date(2026, 10, 17),
    30: date(2026, 11, 1),
}

TOUR_FIXTURES = {
    1: [
        ("АҚТОБЕ", "ТОБЫЛ"),
        ("АЛТАЙ ӨСКЕМЕН", "ҚАЙРАТ"),
        ("АСТАНА", "ЖЕТІСУ"),
        ("АТЫРАУ", "ҰЛЫТАУ"),
        ("ҚАЙСАР", "ЖЕҢІС"),
        ("QYZYLJAR", "КАСПИЙ"),
        ("ОҚЖЕТПЕС", "ЕЛІМАЙ"),
        ("ОРДАБАСЫ", "ЕРТІС"),
    ],
    2: [
        ("КАСПИЙ", "ОҚЖЕТПЕС"),
        ("ЕЛІМАЙ", "АЛТАЙ ӨСКЕМЕН"),
        ("ЕРТІС", "АТЫРАУ"),
        ("ҚАЙРАТ", "АҚТОБЕ"),
        ("ҚАЙСАР", "АСТАНА"),
        ("ТОБЫЛ", "ЖЕТІСУ"),
        ("ҰЛЫТАУ", "QYZYLJAR"),
        ("ЖЕҢІС", "ОРДАБАСЫ"),
    ],
    3: [
        ("АҚТОБЕ", "ЕЛІМАЙ"),
        ("АЛТАЙ ӨСКЕМЕН", "КАСПИЙ"),
        ("АСТАНА", "ТОБЫЛ"),
        ("АТЫРАУ", "ЖЕҢІС"),
        ("QYZYLJAR", "ЕРТІС"),
        ("ОҚЖЕТПЕС", "ҰЛЫТАУ"),
        ("ОРДАБАСЫ", "ҚАЙСАР"),
        ("ЖЕТІСУ", "ҚАЙРАТ"),
    ],
    4: [
        ("КАСПИЙ", "ЖЕТІСУ"),
        ("ЕЛІМАЙ", "ТОБЫЛ"),
        ("ЕРТІС", "АЛТАЙ ӨСКЕМЕН"),
        ("ҚАЙРАТ", "АСТАНА"),
        ("ҚАЙСАР", "QYZYLJAR"),
        ("ОРДАБАСЫ", "АТЫРАУ"),
        ("ҰЛЫТАУ", "АҚТОБЕ"),
        ("ЖЕҢІС", "ОҚЖЕТПЕС"),
    ],
    5: [
        ("АҚТОБЕ", "КАСПИЙ"),
        ("АЛТАЙ ӨСКЕМЕН", "ҰЛЫТАУ"),
        ("АСТАНА", "ОРДАБАСЫ"),
        ("АТЫРАУ", "ҚАЙСАР"),
        ("QYZYLJAR", "ЖЕҢІС"),
        ("ОҚЖЕТПЕС", "ЕРТІС"),
        ("ТОБЫЛ", "ҚАЙРАТ"),
        ("ЖЕТІСУ", "ЕЛІМАЙ"),
    ],
    6: [
        ("АТЫРАУ", "QYZYLJAR"),
        ("КАСПИЙ", "ҚАЙРАТ"),
        ("ЕЛІМАЙ", "АСТАНА"),
        ("ЕРТІС", "ЖЕТІСУ"),
        ("ҚАЙСАР", "АЛТАЙ ӨСКЕМЕН"),
        ("ОРДАБАСЫ", "ОҚЖЕТПЕС"),
        ("ҰЛЫТАУ", "ТОБЫЛ"),
        ("ЖЕҢІС", "АҚТОБЕ"),
    ],
    7: [
        ("АҚТОБЕ", "ЕРТІС"),
        ("АЛТАЙ ӨСКЕМЕН", "ЖЕҢІС"),
        ("АСТАНА", "АТЫРАУ"),
        ("QYZYLJAR", "ОРДАБАСЫ"),
        ("ҚАЙРАТ", "ЕЛІМАЙ"),
        ("ОҚЖЕТПЕС", "ҚАЙСАР"),
        ("ТОБЫЛ", "КАСПИЙ"),
        ("ЖЕТІСУ", "ҰЛЫТАУ"),
    ],
    8: [
        ("АТЫРАУ", "АЛТАЙ ӨСКЕМЕН"),
        ("КАСПИЙ", "АСТАНА"),
        ("ЕРТІС", "ҚАЙРАТ"),
        ("ҚАЙСАР", "ЖЕТІСУ"),
        ("QYZYLJAR", "ОҚЖЕТПЕС"),
        ("ОРДАБАСЫ", "АҚТОБЕ"),
        ("ҰЛЫТАУ", "ЕЛІМАЙ"),
        ("ЖЕҢІС", "ТОБЫЛ"),
    ],
    9: [
        ("АҚТОБЕ", "ҚАЙСАР"),
        ("АЛТАЙ ӨСКЕМЕН", "ОРДАБАСЫ"),
        ("АСТАНА", "QYZYLJAR"),
        ("ЕЛІМАЙ", "КАСПИЙ"),
        ("ҚАЙРАТ", "ҰЛЫТАУ"),
        ("ОҚЖЕТПЕС", "АТЫРАУ"),
        ("ТОБЫЛ", "ЕРТІС"),
        ("ЖЕТІСУ", "ЖЕҢІС"),
    ],
    10: [
        ("АТЫРАУ", "АҚТОБЕ"),
        ("ЕРТІС", "ЕЛІМАЙ"),
        ("ҚАЙСАР", "ТОБЫЛ"),
        ("QYZYLJAR", "АЛТАЙ ӨСКЕМЕН"),
        ("ОҚЖЕТПЕС", "АСТАНА"),
        ("ОРДАБАСЫ", "ЖЕТІСУ"),
        ("ҰЛЫТАУ", "КАСПИЙ"),
        ("ЖЕҢІС", "ҚАЙРАТ"),
    ],
    11: [
        ("АҚТОБЕ", "QYZYLJAR"),
        ("АЛТАЙ ӨСКЕМЕН", "ОҚЖЕТПЕС"),
        ("АСТАНА", "ҰЛЫТАУ"),
        ("КАСПИЙ", "ЕРТІС"),
        ("ЕЛІМАЙ", "ЖЕҢІС"),
        ("ҚАЙРАТ", "ҚАЙСАР"),
        ("ТОБЫЛ", "ОРДАБАСЫ"),
        ("ЖЕТІСУ", "АТЫРАУ"),
    ],
    12: [
        ("АЛТАЙ ӨСКЕМЕН", "АСТАНА"),
        ("АТЫРАУ", "ТОБЫЛ"),
        ("ЕРТІС", "ҰЛЫТАУ"),
        ("ҚАЙСАР", "ЕЛІМАЙ"),
        ("QYZYLJAR", "ЖЕТІСУ"),
        ("ОҚЖЕТПЕС", "АҚТОБЕ"),
        ("ОРДАБАСЫ", "ҚАЙРАТ"),
        ("ЖЕҢІС", "КАСПИЙ"),
    ],
    13: [
        ("АҚТОБЕ", "АЛТАЙ ӨСКЕМЕН"),
        ("АСТАНА", "ЕРТІС"),
        ("КАСПИЙ", "ҚАЙСАР"),
        ("ЕЛІМАЙ", "ОРДАБАСЫ"),
        ("ҚАЙРАТ", "АТЫРАУ"),
        ("ТОБЫЛ", "QYZYLJAR"),
        ("ҰЛЫТАУ", "ЖЕҢІС"),
        ("ЖЕТІСУ", "ОҚЖЕТПЕС"),
    ],
    14: [
        ("АҚТОБЕ", "АСТАНА"),
        ("АЛТАЙ ӨСКЕМЕН", "ЖЕТІСУ"),
        ("АТЫРАУ", "ЕЛІМАЙ"),
        ("ҚАЙСАР", "ҰЛЫТАУ"),
        ("QYZYLJAR", "ҚАЙРАТ"),
        ("ОҚЖЕТПЕС", "ТОБЫЛ"),
        ("ОРДАБАСЫ", "КАСПИЙ"),
        ("ЖЕҢІС", "ЕРТІС"),
    ],
    15: [
        ("КАСПИЙ", "АТЫРАУ"),
        ("ЕЛІМАЙ", "QYZYLJAR"),
        ("ЕРТІС", "ҚАЙСАР"),
        ("ҚАЙРАТ", "ОҚЖЕТПЕС"),
        ("ТОБЫЛ", "АЛТАЙ ӨСКЕМЕН"),
        ("ҰЛЫТАУ", "ОРДАБАСЫ"),
        ("ЖЕҢІС", "АСТАНА"),
        ("ЖЕТІСУ", "АҚТОБЕ"),
    ],
    16: [
        ("АҚТОБЕ", "ЖЕТІСУ"),
        ("АЛТАЙ ӨСКЕМЕН", "ТОБЫЛ"),
        ("АСТАНА", "ЖЕҢІС"),
        ("АТЫРАУ", "КАСПИЙ"),
        ("ҚАЙСАР", "ЕРТІС"),
        ("QYZYLJAR", "ЕЛІМАЙ"),
        ("ОҚЖЕТПЕС", "ҚАЙРАТ"),
        ("ОРДАБАСЫ", "ҰЛЫТАУ"),
    ],
    17: [
        ("АСТАНА", "АҚТОБЕ"),
        ("КАСПИЙ", "ОРДАБАСЫ"),
        ("ЕЛІМАЙ", "АТЫРАУ"),
        ("ЕРТІС", "ЖЕҢІС"),
        ("ҚАЙРАТ", "QYZYLJAR"),
        ("ТОБЫЛ", "ОҚЖЕТПЕС"),
        ("ҰЛЫТАУ", "ҚАЙСАР"),
        ("ЖЕТІСУ", "АЛТАЙ ӨСКЕМЕН"),
    ],
    18: [
        ("АЛТАЙ ӨСКЕМЕН", "АҚТОБЕ"),
        ("АТЫРАУ", "ҚАЙРАТ"),
        ("ЕРТІС", "АСТАНА"),
        ("ҚАЙСАР", "КАСПИЙ"),
        ("QYZYLJAR", "ТОБЫЛ"),
        ("ОҚЖЕТПЕС", "ЖЕТІСУ"),
        ("ОРДАБАСЫ", "ЕЛІМАЙ"),
        ("ЖЕҢІС", "ҰЛЫТАУ"),
    ],
    19: [
        ("АҚТОБЕ", "ОҚЖЕТПЕС"),
        ("АСТАНА", "АЛТАЙ ӨСКЕМЕН"),
        ("КАСПИЙ", "ЖЕҢІС"),
        ("ЕЛІМАЙ", "ҚАЙСАР"),
        ("ҚАЙРАТ", "ОРДАБАСЫ"),
        ("ТОБЫЛ", "АТЫРАУ"),
        ("ҰЛЫТАУ", "ЕРТІС"),
        ("ЖЕТІСУ", "QYZYLJAR"),
    ],
    20: [
        ("АТЫРАУ", "ЖЕТІСУ"),
        ("ЕРТІС", "КАСПИЙ"),
        ("ҚАЙСАР", "ҚАЙРАТ"),
        ("QYZYLJAR", "АҚТОБЕ"),
        ("ОҚЖЕТПЕС", "АЛТАЙ ӨСКЕМЕН"),
        ("ОРДАБАСЫ", "ТОБЫЛ"),
        ("ҰЛЫТАУ", "АСТАНА"),
        ("ЖЕҢІС", "ЕЛІМАЙ"),
    ],
    21: [
        ("АҚТОБЕ", "АТЫРАУ"),
        ("АЛТАЙ ӨСКЕМЕН", "QYZYLJAR"),
        ("АСТАНА", "ОҚЖЕТПЕС"),
        ("КАСПИЙ", "ҰЛЫТАУ"),
        ("ЕЛІМАЙ", "ЕРТІС"),
        ("ҚАЙРАТ", "ЖЕҢІС"),
        ("ТОБЫЛ", "ҚАЙСАР"),
        ("ЖЕТІСУ", "ОРДАБАСЫ"),
    ],
    22: [
        ("АТЫРАУ", "ОҚЖЕТПЕС"),
        ("КАСПИЙ", "ЕЛІМАЙ"),
        ("ЕРТІС", "ТОБЫЛ"),
        ("ҚАЙСАР", "АҚТОБЕ"),
        ("QYZYLJAR", "АСТАНА"),
        ("ОРДАБАСЫ", "АЛТАЙ ӨСКЕМЕН"),
        ("ҰЛЫТАУ", "ҚАЙРАТ"),
        ("ЖЕҢІС", "ЖЕТІСУ"),
    ],
    23: [
        ("АҚТОБЕ", "ОРДАБАСЫ"),
        ("АЛТАЙ ӨСКЕМЕН", "АТЫРАУ"),
        ("АСТАНА", "КАСПИЙ"),
        ("ЕЛІМАЙ", "ҰЛЫТАУ"),
        ("ҚАЙРАТ", "ЕРТІС"),
        ("ОҚЖЕТПЕС", "QYZYLJAR"),
        ("ТОБЫЛ", "ЖЕҢІС"),
        ("ЖЕТІСУ", "ҚАЙСАР"),
    ],
    24: [
        ("АТЫРАУ", "АСТАНА"),
        ("КАСПИЙ", "ТОБЫЛ"),
        ("ЕЛІМАЙ", "ҚАЙРАТ"),
        ("ЕРТІС", "АҚТОБЕ"),
        ("ҚАЙСАР", "ОҚЖЕТПЕС"),
        ("ОРДАБАСЫ", "QYZYLJAR"),
        ("ҰЛЫТАУ", "ЖЕТІСУ"),
        ("ЖЕҢІС", "АЛТАЙ ӨСКЕМЕН"),
    ],
    25: [
        ("АҚТОБЕ", "ЖЕҢІС"),
        ("АЛТАЙ ӨСКЕМЕН", "ҚАЙСАР"),
        ("АСТАНА", "ЕЛІМАЙ"),
        ("QYZYLJAR", "АТЫРАУ"),
        ("ҚАЙРАТ", "КАСПИЙ"),
        ("ОҚЖЕТПЕС", "ОРДАБАСЫ"),
        ("ТОБЫЛ", "ҰЛЫТАУ"),
        ("ЖЕТІСУ", "ЕРТІС"),
    ],
    26: [
        ("КАСПИЙ", "АҚТОБЕ"),
        ("ЕЛІМАЙ", "ЖЕТІСУ"),
        ("ЕРТІС", "ОҚЖЕТПЕС"),
        ("ҚАЙРАТ", "ТОБЫЛ"),
        ("ҚАЙСАР", "АТЫРАУ"),
        ("ОРДАБАСЫ", "АСТАНА"),
        ("ҰЛЫТАУ", "АЛТАЙ ӨСКЕМЕН"),
        ("ЖЕҢІС", "QYZYLJAR"),
    ],
    27: [
        ("АҚТОБЕ", "ҰЛЫТАУ"),
        ("АЛТАЙ ӨСКЕМЕН", "ЕРТІС"),
        ("АСТАНА", "ҚАЙРАТ"),
        ("АТЫРАУ", "ОРДАБАСЫ"),
        ("QYZYLJAR", "ҚАЙСАР"),
        ("ОҚЖЕТПЕС", "ЖЕҢІС"),
        ("ТОБЫЛ", "ЕЛІМАЙ"),
        ("ЖЕТІСУ", "КАСПИЙ"),
    ],
    28: [
        ("КАСПИЙ", "АЛТАЙ ӨСКЕМЕН"),
        ("ЕЛІМАЙ", "АҚТОБЕ"),
        ("ЕРТІС", "QYZYLJAR"),
        ("ҚАЙРАТ", "ЖЕТІСУ"),
        ("ҚАЙСАР", "ОРДАБАСЫ"),
        ("ТОБЫЛ", "АСТАНА"),
        ("ҰЛЫТАУ", "ОҚЖЕТПЕС"),
        ("ЖЕҢІС", "АТЫРАУ"),
    ],
    29: [
        ("АҚТОБЕ", "ҚАЙРАТ"),
        ("АЛТАЙ ӨСКЕМЕН", "ЕЛІМАЙ"),
        ("АСТАНА", "ҚАЙСАР"),
        ("АТЫРАУ", "ЕРТІС"),
        ("QYZYLJAR", "ҰЛЫТАУ"),
        ("ОҚЖЕТПЕС", "КАСПИЙ"),
        ("ОРДАБАСЫ", "ЖЕҢІС"),
        ("ЖЕТІСУ", "ТОБЫЛ"),
    ],
    30: [
        ("КАСПИЙ", "QYZYLJAR"),
        ("ЕЛІМАЙ", "ОҚЖЕТПЕС"),
        ("ЕРТІС", "ОРДАБАСЫ"),
        ("ҚАЙРАТ", "АЛТАЙ ӨСКЕМЕН"),
        ("ТОБЫЛ", "АҚТОБЕ"),
        ("ҰЛЫТАУ", "АТЫРАУ"),
        ("ЖЕҢІС", "ҚАЙСАР"),
        ("ЖЕТІСУ", "АСТАНА"),
    ],
}

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

TOKEN_ALIASES = {
    "актобе": ("актобе",),
    "тобыл": ("тобыл",),
    "алтаи оскемен": ("алтай оскемен", "алтай"),
    "астана": ("астана",),
    "атырау": ("атырау",),
    "улытау": ("улытау",),
    "каисар": ("кайсар",),
    "женис": ("женис",),
    "qyzyljar": ("qyzyljar", "кызылжар"),
    "каспии": ("каспий",),
    "окжетпес": ("окжетпес",),
    "елимаи": ("елимай",),
    "ордабасы": ("ордабасы",),
    "ертис": ("ертис", "иртыш", "ertis"),
    "каират": ("кайрат",),
    "жетису": ("жетису", "жетысу"),
}


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
    normalized_token = _normalize_name(token)

    exact_matches = {
        team_id
        for team_id, names in names_by_team_id.items()
        if normalized_token in names
    }
    if len(exact_matches) == 1:
        return next(iter(exact_matches))

    aliases = tuple(
        _normalize_name(alias)
        for alias in TOKEN_ALIASES.get(normalized_token, (normalized_token,))
    )
    candidates: set[int] = set()
    for team_id, names in names_by_team_id.items():
        if any(any(alias in name for alias in aliases) for name in names):
            candidates.add(team_id)

    if len(candidates) == 1:
        return next(iter(candidates))

    raise ValueError(
        f"Unable to resolve team token '{token}' (normalized='{normalized_token}'). "
        f"Candidates: {sorted(candidates)}"
    )


def _ensure_stage(bind, tour: int) -> int:
    existing_stage_id = bind.execute(
        sa.text(
            """
            SELECT id
            FROM stages
            WHERE season_id = :season_id AND stage_number = :stage_number
            ORDER BY id
            LIMIT 1
            """
        ),
        {"season_id": SEASON_ID, "stage_number": tour},
    ).scalar_one_or_none()

    if existing_stage_id is not None:
        return int(existing_stage_id)

    return int(
        bind.execute(
            sa.text(
                """
                INSERT INTO stages (season_id, name, name_kz, stage_number, sort_order)
                VALUES (:season_id, :name, :name_kz, :stage_number, :sort_order)
                RETURNING id
                """
            ),
            {
                "season_id": SEASON_ID,
                "name": f"Тур {tour}",
                "name_kz": f"{tour} тур",
                "stage_number": tour,
                "sort_order": tour,
            },
        ).scalar_one()
    )


def _supercup_exists(bind, home_team_id: int, away_team_id: int) -> bool:
    existing_id = bind.execute(
        sa.text(
            """
            SELECT id
            FROM games
            WHERE season_id = :season_id
              AND date = :game_date
              AND (
                (home_team_id = :home_team_id AND away_team_id = :away_team_id)
                OR
                (home_team_id = :away_team_id AND away_team_id = :home_team_id)
              )
            ORDER BY id
            LIMIT 1
            """
        ),
        {
            "season_id": SEASON_ID,
            "game_date": SUPER_CUP_DATE,
            "home_team_id": home_team_id,
            "away_team_id": away_team_id,
        },
    ).scalar_one_or_none()

    return existing_id is not None


def upgrade() -> None:
    op.add_column(
        "games",
        sa.Column(
            "is_schedule_tentative",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )

    bind = op.get_bind()
    names_by_team_id = _get_participant_names_by_team_id(bind)

    # Explicitly check supercup presence. The fixture is expected to exist already.
    supercup_home_id = _resolve_team_id(SUPER_CUP_HOME_TOKEN, names_by_team_id)
    supercup_away_id = _resolve_team_id(SUPER_CUP_AWAY_TOKEN, names_by_team_id)
    _supercup_exists(bind, supercup_home_id, supercup_away_id)

    stage_ids: dict[int, int] = {}

    for tour, fixtures in TOUR_FIXTURES.items():
        if tour < 4:
            continue

        stage_id = stage_ids.setdefault(tour, _ensure_stage(bind, tour))
        game_date = TOUR_START_DATES[tour]

        for home_token, away_token in fixtures:
            home_team_id = _resolve_team_id(home_token, names_by_team_id)
            away_team_id = _resolve_team_id(away_token, names_by_team_id)

            existing_id = bind.execute(
                sa.text(
                    """
                    SELECT id
                    FROM games
                    WHERE season_id = :season_id
                      AND tour = :tour
                      AND home_team_id = :home_team_id
                      AND away_team_id = :away_team_id
                    ORDER BY id
                    LIMIT 1
                    """
                ),
                {
                    "season_id": SEASON_ID,
                    "tour": tour,
                    "home_team_id": home_team_id,
                    "away_team_id": away_team_id,
                },
            ).scalar_one_or_none()

            if existing_id is not None:
                # Keep existing factual fields untouched; only fill structural gaps.
                bind.execute(
                    sa.text(
                        """
                        UPDATE games
                        SET stage_id = COALESCE(stage_id, :stage_id),
                            tour = COALESCE(tour, :tour),
                            is_schedule_tentative = true
                        WHERE id = :game_id
                        """
                    ),
                    {
                        "game_id": int(existing_id),
                        "stage_id": stage_id,
                        "tour": tour,
                    },
                )
                continue

            bind.execute(
                sa.text(
                    """
                    INSERT INTO games (
                        season_id,
                        tour,
                        stage_id,
                        home_team_id,
                        away_team_id,
                        date,
                        time,
                        has_stats,
                        has_lineup,
                        is_live,
                        is_technical,
                        is_schedule_tentative,
                        updated_at
                    )
                    VALUES (
                        :season_id,
                        :tour,
                        :stage_id,
                        :home_team_id,
                        :away_team_id,
                        :game_date,
                        NULL,
                        false,
                        false,
                        false,
                        false,
                        true,
                        NOW()
                    )
                    """
                ),
                {
                    "season_id": SEASON_ID,
                    "tour": tour,
                    "stage_id": stage_id,
                    "home_team_id": home_team_id,
                    "away_team_id": away_team_id,
                    "game_date": game_date,
                },
            )

    bind.execute(
        sa.text(
            "UPDATE games SET is_schedule_tentative = true WHERE season_id = :season_id"
        ),
        {"season_id": SEASON_ID},
    )


def downgrade() -> None:
    op.drop_column("games", "is_schedule_tentative")
