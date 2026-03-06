"""Backfill frontend_code on historical seasons so season selector is visible

For each known coded season (61=pl, 85=1l, 71=cup, 80=2l, 84=el), find all
other seasons with the same championship_id that lack frontend_code, and copy
the tournament config from the known season. Also re-enable is_visible for
2024 seasons that have actual game data.

Revision ID: a8b9c0d1e2f3
Revises: z7c8d9e0f1g2
Create Date: 2026-03-06
"""

from alembic import op
import sqlalchemy as sa

revision = "a8b9c0d1e2f3"
down_revision = "z7c8d9e0f1g2"
branch_labels = None
depends_on = None

# Known coded seasons: (season_id, frontend_code)
# Season 200 (2026 PL) already has frontend_code='pl', so it won't be touched.
KNOWN_SEASONS = [
    (61, "pl"),
    (85, "1l"),
    (71, "cup"),
    (80, "2l"),
    (84, "el"),
]


def upgrade() -> None:
    bind = op.get_bind()

    for known_id, _code in KNOWN_SEASONS:
        # Copy tournament config from the known season to all siblings
        # that share the same championship_id but have no frontend_code yet.
        # Done entirely in SQL to avoid asyncpg type-serialization issues.
        bind.execute(
            sa.text("""
                UPDATE seasons AS s
                SET frontend_code    = src.frontend_code,
                    tournament_type  = src.tournament_type,
                    tournament_format = src.tournament_format,
                    has_table        = src.has_table,
                    has_bracket      = src.has_bracket,
                    sponsor_name     = src.sponsor_name,
                    sponsor_name_kz  = src.sponsor_name_kz,
                    logo             = src.logo,
                    sort_order       = src.sort_order,
                    colors           = src.colors
                FROM seasons AS src
                WHERE src.id = :known_id
                  AND s.championship_id = src.championship_id
                  AND s.id != src.id
                  AND s.frontend_code IS NULL
            """),
            {"known_id": known_id},
        )

    # Backfill date_start/date_end from actual game dates for seasons
    # that lack them (needed for front-map to build season_options).
    bind.execute(
        sa.text("""
            UPDATE seasons s
            SET date_start = sub.first_game,
                date_end   = sub.last_game
            FROM (
                SELECT g.season_id, MIN(g.date) AS first_game, MAX(g.date) AS last_game
                FROM games g
                GROUP BY g.season_id
            ) sub
            WHERE sub.season_id = s.id
              AND s.frontend_code IS NOT NULL
              AND s.date_start IS NULL
              AND sub.first_game IS NOT NULL
        """)
    )

    # Make previously-hidden seasons visible — but only those that now have
    # a frontend_code AND contain actual game data.
    bind.execute(
        sa.text("""
            UPDATE seasons
            SET is_visible = true
            WHERE is_visible = false
              AND frontend_code IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM games WHERE games.season_id = seasons.id
              )
        """)
    )


def downgrade() -> None:
    bind = op.get_bind()

    for known_id, code in KNOWN_SEASONS:
        # Remove frontend_code from siblings (not the originally-known season,
        # and not season 200 which was created with its own frontend_code).
        bind.execute(
            sa.text("""
                UPDATE seasons AS s
                SET frontend_code    = NULL,
                    tournament_type  = NULL,
                    tournament_format = NULL,
                    has_table        = false,
                    has_bracket      = false,
                    sponsor_name     = NULL,
                    sponsor_name_kz  = NULL,
                    logo             = NULL,
                    sort_order       = 0,
                    colors           = NULL
                FROM seasons AS src
                WHERE src.id = :known_id
                  AND s.championship_id = src.championship_id
                  AND s.id != src.id
                  AND s.id != 200
                  AND s.frontend_code = :code
            """),
            {"known_id": known_id, "code": code},
        )

    # Re-hide 2024 seasons (matches the original x1y2z3a4b5c6 logic)
    bind.execute(
        sa.text("""
            UPDATE seasons
            SET is_visible = false
            WHERE (
                date_start >= DATE '2024-01-01'
                AND date_start < DATE '2025-01-01'
            )
            OR (
                date_start IS NULL
                AND (
                    COALESCE(name, '') ILIKE '%2024%'
                    OR COALESCE(name_kz, '') ILIKE '%2024%'
                    OR COALESCE(name_en, '') ILIKE '%2024%'
                )
            )
        """)
    )
