"""add 21 new metric columns to game_team_stats

Revision ID: zm9n0o1p2q3r4
Revises: zl8n9o0p1q2r3
Create Date: 2026-03-19 12:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "zm9n0o1p2q3r4"
down_revision: Union[str, None] = "zl8n9o0p1q2r3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Count columns (Integer)
    op.add_column("game_team_stats", sa.Column("minutes", sa.Integer(), nullable=True))
    op.add_column("game_team_stats", sa.Column("xg", sa.Numeric(6, 2), nullable=True))
    op.add_column("game_team_stats", sa.Column("freekicks", sa.Integer(), nullable=True))
    op.add_column("game_team_stats", sa.Column("freekick_shots", sa.Integer(), nullable=True))
    op.add_column("game_team_stats", sa.Column("freekick_passes", sa.Integer(), nullable=True))
    op.add_column("game_team_stats", sa.Column("throw_ins", sa.Integer(), nullable=True))
    op.add_column("game_team_stats", sa.Column("goal_kicks", sa.Integer(), nullable=True))
    op.add_column("game_team_stats", sa.Column("assists", sa.Integer(), nullable=True))
    op.add_column("game_team_stats", sa.Column("passes_forward", sa.Integer(), nullable=True))
    op.add_column("game_team_stats", sa.Column("passes_progressive", sa.Integer(), nullable=True))
    op.add_column("game_team_stats", sa.Column("key_passes", sa.Integer(), nullable=True))
    op.add_column("game_team_stats", sa.Column("passes_to_final_third", sa.Integer(), nullable=True))
    op.add_column("game_team_stats", sa.Column("passes_to_box", sa.Integer(), nullable=True))
    op.add_column("game_team_stats", sa.Column("crosses", sa.Integer(), nullable=True))

    # Ratio columns (Numeric 5,2)
    op.add_column("game_team_stats", sa.Column("shot_accuracy", sa.Numeric(5, 2), nullable=True))
    op.add_column("game_team_stats", sa.Column("corner_accuracy", sa.Numeric(5, 2), nullable=True))
    op.add_column("game_team_stats", sa.Column("freekick_shot_accuracy", sa.Numeric(5, 2), nullable=True))
    op.add_column("game_team_stats", sa.Column("freekick_pass_accuracy", sa.Numeric(5, 2), nullable=True))
    op.add_column("game_team_stats", sa.Column("throw_in_accuracy", sa.Numeric(5, 2), nullable=True))
    op.add_column("game_team_stats", sa.Column("goal_kick_accuracy", sa.Numeric(5, 2), nullable=True))
    op.add_column("game_team_stats", sa.Column("penalty_accuracy", sa.Numeric(5, 2), nullable=True))


def downgrade() -> None:
    op.drop_column("game_team_stats", "penalty_accuracy")
    op.drop_column("game_team_stats", "goal_kick_accuracy")
    op.drop_column("game_team_stats", "throw_in_accuracy")
    op.drop_column("game_team_stats", "freekick_pass_accuracy")
    op.drop_column("game_team_stats", "freekick_shot_accuracy")
    op.drop_column("game_team_stats", "corner_accuracy")
    op.drop_column("game_team_stats", "shot_accuracy")
    op.drop_column("game_team_stats", "crosses")
    op.drop_column("game_team_stats", "passes_to_box")
    op.drop_column("game_team_stats", "passes_to_final_third")
    op.drop_column("game_team_stats", "key_passes")
    op.drop_column("game_team_stats", "passes_progressive")
    op.drop_column("game_team_stats", "passes_forward")
    op.drop_column("game_team_stats", "assists")
    op.drop_column("game_team_stats", "goal_kicks")
    op.drop_column("game_team_stats", "throw_ins")
    op.drop_column("game_team_stats", "freekick_passes")
    op.drop_column("game_team_stats", "freekick_shots")
    op.drop_column("game_team_stats", "freekicks")
    op.drop_column("game_team_stats", "xg")
    op.drop_column("game_team_stats", "minutes")
