"""Set sota_season_id for 1L/2L 2026 and add sota_season_ids text field.

Revision ID: zs5t6u7v8w9x0
Revises: zr4s5t6u7v8w9
Create Date: 2026-04-01
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "zs5t6u7v8w9x0"
down_revision: Union[str, None] = "zr4s5t6u7v8w9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# SOTA season IDs from https://sota.id/api/public/v1/seasons/
# Season 174 = First League 2026 (tournament 30)
# Season 181 = Second League Southwest 2026 (tournament 75)
# Season 182 = Second League Northeast 2026 (tournament 74)

LEAGUE1_LOCAL_ID = 204
LEAGUE1_SOTA_ID = 174

LEAGUE2_LOCAL_ID = 203
LEAGUE2_SOTA_PRIMARY = 181  # SW conference
LEAGUE2_SOTA_ALL = "181;182"  # SW + NE conferences


def upgrade() -> None:
    conn = op.get_bind()

    # 1. Add sota_season_ids TEXT column (semicolon-separated, like Championship.sota_ids)
    existing = {
        row[0]
        for row in conn.execute(
            sa.text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'seasons'"
            )
        )
    }
    if "sota_season_ids" not in existing:
        op.add_column(
            "seasons", sa.Column("sota_season_ids", sa.Text(), nullable=True)
        )

    # 2. Set sota_season_id for First League 2026
    conn.execute(
        sa.text("UPDATE seasons SET sota_season_id = :sota WHERE id = :local"),
        {"sota": LEAGUE1_SOTA_ID, "local": LEAGUE1_LOCAL_ID},
    )

    # 3. Set sota_season_id for Second League 2026 (primary = SW)
    conn.execute(
        sa.text("UPDATE seasons SET sota_season_id = :sota WHERE id = :local"),
        {"sota": LEAGUE2_SOTA_PRIMARY, "local": LEAGUE2_LOCAL_ID},
    )

    # 4. Set sota_season_ids for Second League 2026 (both conferences)
    conn.execute(
        sa.text("UPDATE seasons SET sota_season_ids = :ids WHERE id = :local"),
        {"ids": LEAGUE2_SOTA_ALL, "local": LEAGUE2_LOCAL_ID},
    )

    # 5. Backfill sota_season_ids for all seasons that have sota_season_id but no sota_season_ids
    conn.execute(
        sa.text(
            "UPDATE seasons SET sota_season_ids = CAST(sota_season_id AS TEXT) "
            "WHERE sota_season_id IS NOT NULL AND sota_season_ids IS NULL"
        )
    )


def downgrade() -> None:
    conn = op.get_bind()

    # Clear the new SOTA mappings
    conn.execute(
        sa.text("UPDATE seasons SET sota_season_id = NULL WHERE id IN (203, 204)")
    )

    # Drop the column
    op.drop_column("seasons", "sota_season_ids")
