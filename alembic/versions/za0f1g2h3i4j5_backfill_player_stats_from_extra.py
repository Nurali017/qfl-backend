"""backfill pass_accuracy and minutes_played from extra_stats

Revision ID: za0f1g2h3i4j5
Revises: z9e0f1g2h3i4
Create Date: 2026-03-12 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "za0f1g2h3i4j5"
down_revision: Union[str, None] = "tk1a2b3c4d5e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        UPDATE game_player_stats SET
            minutes_played = (extra_stats->>'time_on_field_total')::int
        WHERE minutes_played IS NULL
          AND extra_stats->>'time_on_field_total' IS NOT NULL
    """)
    op.execute("""
        UPDATE game_player_stats SET
            pass_accuracy = (extra_stats->>'pass_ratio')::numeric
        WHERE pass_accuracy IS NULL
          AND extra_stats->>'pass_ratio' IS NOT NULL
    """)
    op.execute("""
        UPDATE game_team_stats SET
            pass_accuracy = (extra_stats->>'pass_ratio')::numeric
        WHERE pass_accuracy IS NULL
          AND extra_stats->>'pass_ratio' IS NOT NULL
    """)


def downgrade() -> None:
    # Data backfill — no structural rollback needed
    pass
