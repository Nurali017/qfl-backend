"""Add 2026 PL season with participants, stages, and games

Revision ID: d7e8f9g0h1i2
Revises: c6d7e8f9g0h1
Create Date: 2026-02-23 12:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from datetime import date

revision = "d7e8f9g0h1i2"
down_revision = "c6d7e8f9g0h1"
branch_labels = None
depends_on = None

SEASON_ID = 200

# 13 teams with known IDs
KNOWN_TEAM_IDS = [51, 90, 13, 91, 45, 49, 94, 87, 318, 81, 92, 93, 293]

# Tour schedule: list of (home_team, away_team, date) per tour
# For looked-up teams, we use placeholder strings that get resolved at runtime
TOURS = {
    1: [
        (51, 90, date(2026, 3, 7)),
        ("altai", 13, date(2026, 3, 7)),
        (91, 45, date(2026, 3, 7)),
        (49, 293, date(2026, 3, 7)),
        (94, 92, date(2026, 3, 8)),
        (87, "kaspiy", date(2026, 3, 8)),
        (318, 93, date(2026, 3, 8)),
        (81, "ertis", date(2026, 3, 8)),
    ],
    2: [
        ("kaspiy", 318, date(2026, 3, 14)),
        (93, "altai", date(2026, 3, 14)),
        ("ertis", 49, date(2026, 3, 14)),
        (13, 51, date(2026, 3, 14)),
        (94, 91, date(2026, 3, 15)),
        (90, 45, date(2026, 3, 15)),
        (293, 87, date(2026, 3, 15)),
        (92, 81, date(2026, 3, 15)),
    ],
    3: [
        (51, 93, date(2026, 3, 19)),
        ("altai", "kaspiy", date(2026, 3, 19)),
        (91, 90, date(2026, 3, 19)),
        (49, 92, date(2026, 3, 19)),
        (87, "ertis", date(2026, 3, 20)),
        (318, 293, date(2026, 3, 20)),
        (81, 94, date(2026, 3, 20)),
        (45, 13, date(2026, 3, 20)),
    ],
}


def upgrade() -> None:
    bind = op.get_bind()

    # --- Look up 3 teams by name (create if missing) ---
    altai_id = bind.execute(
        sa.text("SELECT id FROM teams WHERE name ILIKE '%Алтай%' AND name NOT ILIKE '%М' LIMIT 1")
    ).scalar_one()

    kaspiy_id = bind.execute(
        sa.text("SELECT id FROM teams WHERE name ILIKE '%Каспий%' AND name NOT ILIKE '%М' LIMIT 1")
    ).scalar_one()

    ertis_id = bind.execute(
        sa.text(
            "SELECT id FROM teams WHERE name ILIKE '%Иртыш%' OR name ILIKE '%Ертіс%' OR name ILIKE '%Ertis%' OR name ILIKE '%Ertіs%' LIMIT 1"
        )
    ).scalar_one_or_none()

    if ertis_id is None:
        ertis_id = bind.execute(
            sa.text(
                "INSERT INTO teams (name, name_kz, updated_at) "
                "VALUES ('Ертіс', 'Ертіс', NOW()) RETURNING id"
            )
        ).scalar_one()

    team_lookup = {
        "altai": altai_id,
        "kaspiy": kaspiy_id,
        "ertis": ertis_id,
    }

    def resolve(team):
        return team_lookup[team] if isinstance(team, str) else team

    # --- 1a. Copy championship_id from season 61, then insert season 200 ---
    champ_id = bind.execute(
        sa.text("SELECT championship_id FROM seasons WHERE id = 61")
    ).scalar_one()

    # Use inline values for dates/JSON to avoid asyncpg type issues
    bind.execute(
        sa.text("""
            INSERT INTO seasons (
                id, championship_id, name, name_kz,
                date_start, date_end,
                frontend_code, tournament_type, tournament_format,
                has_table, has_bracket,
                sponsor_name, sponsor_name_kz,
                logo, sort_order, colors,
                current_round, total_rounds,
                sync_enabled, updated_at
            ) VALUES (
                :id, :champ_id, :name, :name_kz,
                '2026-03-07'::date, '2026-11-30'::date,
                'pl', 'league', 'round_robin',
                true, false,
                'ПРЕМЬЕР-ЛИГА', 'ПРЕМЬЕР-ЛИГА',
                '/images/tournaments/pl.png', 1,
                '{"primary": "30 77 140", "primaryLight": "42 95 163", "primaryDark": "22 58 107", "accent": "229 183 59", "accentSoft": "240 201 93"}'::jsonb,
                NULL, NULL,
                false, NOW()
            )
        """),
        {
            "id": SEASON_ID,
            "champ_id": champ_id,
            "name": "Премьер-Лига 2026",
            "name_kz": "Премьер-Лига 2026",
        },
    )

    # --- 1b. Create 16 season_participants ---
    all_team_ids = KNOWN_TEAM_IDS + [altai_id, kaspiy_id, ertis_id]
    for team_id in all_team_ids:
        bind.execute(
            sa.text(
                "INSERT INTO season_participants (team_id, season_id) "
                "VALUES (:team_id, :season_id)"
            ),
            {"team_id": team_id, "season_id": SEASON_ID},
        )

    # --- 1c. Create 3 stages ---
    stage_ids = {}
    for tour_num in range(1, 4):
        result = bind.execute(
            sa.text("""
                INSERT INTO stages (season_id, name, name_kz, stage_number, sort_order)
                VALUES (:season_id, :name, :name_kz, :stage_number, :sort_order)
                RETURNING id
            """),
            {
                "season_id": SEASON_ID,
                "name": f"Тур {tour_num}",
                "name_kz": f"{tour_num} тур",
                "stage_number": tour_num,
                "sort_order": tour_num,
            },
        )
        stage_ids[tour_num] = result.scalar_one()

    # --- 1d. Create 24 games (8 per tour) ---
    for tour_num, games in TOURS.items():
        for home, away, game_date in games:
            bind.execute(
                sa.text("""
                    INSERT INTO games (season_id, tour, stage_id, home_team_id, away_team_id, date, has_stats, has_lineup, is_live, is_technical, updated_at)
                    VALUES (:season_id, :tour, :stage_id, :home_team_id, :away_team_id, :date, false, false, false, false, NOW())
                """),
                {
                    "season_id": SEASON_ID,
                    "tour": tour_num,
                    "stage_id": stage_ids[tour_num],
                    "home_team_id": resolve(home),
                    "away_team_id": resolve(away),
                    "date": game_date,
                },
            )


def downgrade() -> None:
    bind = op.get_bind()

    # Delete games, stages, participants, and season in order (FK constraints)
    bind.execute(
        sa.text("DELETE FROM games WHERE season_id = :sid"),
        {"sid": SEASON_ID},
    )
    bind.execute(
        sa.text("DELETE FROM stages WHERE season_id = :sid"),
        {"sid": SEASON_ID},
    )
    bind.execute(
        sa.text("DELETE FROM season_participants WHERE season_id = :sid"),
        {"sid": SEASON_ID},
    )
    bind.execute(
        sa.text("DELETE FROM seasons WHERE id = :sid"),
        {"sid": SEASON_ID},
    )
    # Remove Ертіс team if it was created by this migration (no other references)
    bind.execute(
        sa.text(
            "DELETE FROM teams WHERE name = 'Ертіс' AND id NOT IN "
            "(SELECT DISTINCT home_team_id FROM games UNION SELECT DISTINCT away_team_id FROM games)"
        )
    )
