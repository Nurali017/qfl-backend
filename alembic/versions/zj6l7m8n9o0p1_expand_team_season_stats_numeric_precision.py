"""expand team season stats numeric precision

Revision ID: zj6l7m8n9o0p1
Revises: zi5k6l7m8n9o0
Create Date: 2026-03-18 15:30:00.000000
"""

from collections.abc import Iterable

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "zj6l7m8n9o0p1"
down_revision = "zi5k6l7m8n9o0"
branch_labels = None
depends_on = None


NUMERIC_COLUMNS: list[tuple[str, int]] = [
    ("xg", 6),
    ("xg_per_match", 4),
    ("opponent_xg", 6),
    ("shots_on_goal_per_match", 4),
    ("shot_per_90", 4),
    ("shot_per_match", 4),
    ("shot_to_goal", 5),
    ("goal_to_shot_ratio", 5),
    ("possession_percent_average", 5),
    ("pass_per_match", 6),
    ("pass_ratio", 5),
    ("pass_forward_per_match", 5),
    ("pass_forward_ratio", 5),
    ("pass_long_per_match", 5),
    ("pass_long_ratio", 5),
    ("pass_progressive_per_match", 5),
    ("pass_cross_per_match", 5),
    ("pass_cross_ratio", 5),
    ("pass_to_box_per_match", 5),
    ("pass_to_box_ratio", 5),
    ("pass_to_3rd_per_match", 5),
    ("pass_to_3rd_ratio", 5),
    ("key_pass_per_match", 4),
    ("key_pass_ratio", 5),
    ("freekick_pass_per_match", 4),
    ("freekick_shot_per_match", 4),
    ("duel_per_match", 5),
    ("duel_ratio", 5),
    ("aerial_duel_offence_per_match", 4),
    ("aerial_duel_offence_ratio", 5),
    ("aerial_duel_defence_per_match", 4),
    ("aerial_duel_defence_ratio", 5),
    ("ground_duel_offence_per_match", 4),
    ("ground_duel_offence_ratio", 5),
    ("ground_duel_defence_per_match", 4),
    ("ground_duel_defence_ratio", 5),
    ("tackle_per_match", 4),
    ("tackle1_1_ratio", 5),
    ("interception_per_match", 4),
    ("recovery_per_match", 4),
    ("dribble_per_match", 4),
    ("dribble_ratio", 5),
    ("penalty_ratio", 5),
    ("save_penalty_ratio", 5),
    ("corner_per_match", 4),
    ("average_visitors", 8),
]


def _alter_columns(columns: Iterable[tuple[str, int]], target_precision: int) -> None:
    for column_name, existing_precision in columns:
        op.alter_column(
            "team_season_stats",
            column_name,
            existing_type=sa.Numeric(precision=existing_precision, scale=2),
            type_=sa.Numeric(precision=target_precision, scale=2),
            existing_nullable=True,
        )


def upgrade() -> None:
    _alter_columns(NUMERIC_COLUMNS, 10)


def downgrade() -> None:
    for column_name, existing_precision in NUMERIC_COLUMNS:
        op.alter_column(
            "team_season_stats",
            column_name,
            existing_type=sa.Numeric(precision=10, scale=2),
            type_=sa.Numeric(precision=existing_precision, scale=2),
            existing_nullable=True,
        )
