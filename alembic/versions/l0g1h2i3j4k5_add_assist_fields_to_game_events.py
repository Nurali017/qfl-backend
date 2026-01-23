"""add_assist_fields_to_game_events

Revision ID: l0g1h2i3j4k5
Revises: k9f0g1h2i3j4
Create Date: 2026-01-19 23:00:00.000000

Adds assist_player_id and assist_player_name to game_events table
so goals can directly reference the player who assisted.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = 'l0g1h2i3j4k5'
down_revision: Union[str, None] = 'k9f0g1h2i3j4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('game_events', sa.Column('assist_player_id', postgresql.UUID(as_uuid=True), nullable=True))
    op.add_column('game_events', sa.Column('assist_player_name', sa.String(255), nullable=True))
    op.create_foreign_key(
        'fk_game_events_assist_player',
        'game_events', 'players',
        ['assist_player_id'], ['id']
    )


def downgrade() -> None:
    op.drop_constraint('fk_game_events_assist_player', 'game_events', type_='foreignkey')
    op.drop_column('game_events', 'assist_player_name')
    op.drop_column('game_events', 'assist_player_id')
