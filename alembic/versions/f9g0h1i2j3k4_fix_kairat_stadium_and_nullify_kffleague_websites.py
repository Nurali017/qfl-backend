"""Fix Kairat stadium name and nullify kffleague website URLs

1. Kairat (team 13) stadium has no KZ/EN name — users only see
   the Russian "Центральный стадион". Add name_kz and name_en.
2. Some teams have kffleague URLs in their website field, which
   is the league site, not the club's own site. Nullify those.

Revision ID: f9g0h1i2j3k4
Revises: e8f9g0h1i2j3
Create Date: 2026-02-23 20:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = "f9g0h1i2j3k4"
down_revision = "e8f9g0h1i2j3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()

    # 1. Fix Kairat's stadium name (look up stadium_id from teams)
    bind.execute(
        sa.text(
            "UPDATE stadiums SET name_kz = :name_kz, name_en = :name_en "
            "WHERE id = (SELECT stadium_id FROM teams WHERE id = :team_id) "
            "AND name_kz IS NULL"
        ),
        {"name_kz": "Орталық стадион", "name_en": "Central Stadium", "team_id": 13},
    )

    # 2. Nullify kffleague website URLs
    bind.execute(
        sa.text("UPDATE teams SET website = NULL WHERE website LIKE '%kffleague%'")
    )


def downgrade() -> None:
    bind = op.get_bind()

    # Revert stadium names
    bind.execute(
        sa.text(
            "UPDATE stadiums SET name_kz = NULL, name_en = NULL "
            "WHERE id = (SELECT stadium_id FROM teams WHERE id = :team_id)"
        ),
        {"team_id": 13},
    )

    # Note: cannot restore original kffleague URLs — they are lost on upgrade
