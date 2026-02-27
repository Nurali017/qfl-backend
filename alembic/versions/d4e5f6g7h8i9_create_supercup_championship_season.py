"""create supercup championship and season, move game

Revision ID: d4e5f6g7h8i9
Revises: c3d4e5f6g7h8
Create Date: 2026-02-27
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision: str = "d4e5f6g7h8i9"
down_revision: Union[str, None] = "c3d4e5f6g7h8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()

    # Step 1: Create Championship for Super Cup (idempotent)
    row = conn.execute(
        sa.text("SELECT id FROM championships WHERE slug = 'super-cup'")
    ).fetchone()

    if row:
        champ_id = row[0]
    else:
        result = conn.execute(
            sa.text(
                "INSERT INTO championships "
                "(name, name_kz, name_en, short_name, short_name_kz, slug, sort_order, is_active, created_at, updated_at) "
                "VALUES ("
                "'Суперкубок Казахстана', 'Қазақстан Суперкубогы', 'Kazakhstan Super Cup', "
                "'СК', 'СК', 'super-cup', 6, true, NOW(), NOW()"
                ") RETURNING id"
            )
        )
        champ_id = result.scalar_one()

    # Step 2: Create Season 201 for Super Cup 2026 (idempotent)
    exists = conn.execute(
        sa.text("SELECT 1 FROM seasons WHERE id = 201")
    ).fetchone()

    if not exists:
        conn.execute(
            sa.text(
                "INSERT INTO seasons "
                "(id, championship_id, name, name_kz, name_en, "
                "date_start, date_end, frontend_code, tournament_type, tournament_format, "
                "has_table, has_bracket, logo, sort_order, "
                "is_visible, is_current, sync_enabled, updated_at) "
                "VALUES ("
                "201, :champ_id, 'Суперкубок 2026', 'Суперкубок 2026', 'Super Cup 2026', "
                "'2026-02-28', '2026-02-28', 'sc', 'supercup', 'knockout', "
                "false, false, '/images/tournaments/sc.png', 10, "
                "true, false, false, NOW()"
                ")"
            ),
            {"champ_id": champ_id},
        )

    # Step 3: Move the Super Cup match from PL season 200 to SC season 201
    conn.execute(
        sa.text(
            "UPDATE games SET season_id = 201, updated_at = NOW() "
            "WHERE season_id = 200 AND date = '2026-02-28' "
            "AND ((home_team_id = 13 AND away_team_id = 90) "
            "  OR (home_team_id = 90 AND away_team_id = 13))"
        )
    )


def downgrade() -> None:
    conn = op.get_bind()

    # Move the match back to PL season 200
    conn.execute(
        sa.text(
            "UPDATE games SET season_id = 200, updated_at = NOW() "
            "WHERE season_id = 201 AND date = '2026-02-28' "
            "AND ((home_team_id = 13 AND away_team_id = 90) "
            "  OR (home_team_id = 90 AND away_team_id = 13))"
        )
    )

    # Delete season 201
    conn.execute(sa.text("DELETE FROM seasons WHERE id = 201"))

    # Delete championship by slug
    conn.execute(
        sa.text("DELETE FROM championships WHERE slug = 'super-cup'")
    )
