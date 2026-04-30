"""Set sota_season_id for Women's League 2026 (season 205).

Maps local season 205 (Женская Лига 2026, FCMS competition_ids 3674/3675)
to SOTA season IDs 186 (Конф. А) and 187 (Конф. В), so that
sync_best_players() and sync_player_season_stats() pick up scoring leaders
for both conferences (analogous to Second League 2026 → SOTA 181;182).

SOTA season IDs verified by enumerating /public/v1/players/?season_id=N:
- 186: 200 players, 9 teams, all country=Казахстан, female names
- 187: 213 players, 10 teams, all country=Казахстан, female names
- team_ids disjoint between 186 and 187 (different conferences)

Revision ID: zy1z2a3b4c5d6
Revises: zw9x0y1z2a3b4
Create Date: 2026-04-30
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "zy1z2a3b4c5d6"
down_revision: Union[str, None] = "zw9x0y1z2a3b4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

WL_LOCAL_ID = 205
WL_SOTA_PRIMARY = 186  # Конференция А
WL_SOTA_ALL = "186;187"  # Конф. А + Конф. В


def upgrade() -> None:
    conn = op.get_bind()

    conn.execute(
        sa.text(
            "UPDATE seasons SET sota_season_id = :sota, sota_season_ids = :ids "
            "WHERE id = :local"
        ),
        {"sota": WL_SOTA_PRIMARY, "ids": WL_SOTA_ALL, "local": WL_LOCAL_ID},
    )


def downgrade() -> None:
    conn = op.get_bind()

    conn.execute(
        sa.text(
            "UPDATE seasons SET sota_season_id = NULL, sota_season_ids = NULL "
            "WHERE id = :local"
        ),
        {"local": WL_LOCAL_ID},
    )
