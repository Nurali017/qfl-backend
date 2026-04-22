"""add assist manual override to game events

Revision ID: gg5d6e7f8g9h
Revises: 82ff37c69f96
Create Date: 2026-04-22 21:10:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "gg5d6e7f8g9h"
down_revision: Union[str, None] = "82ff37c69f96"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "game_events",
        sa.Column(
            "assist_manual_override",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )


def downgrade() -> None:
    op.drop_column("game_events", "assist_manual_override")
