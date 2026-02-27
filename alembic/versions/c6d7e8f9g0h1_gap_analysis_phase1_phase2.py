"""gap_analysis_phase1_phase2

Revision ID: c6d7e8f9g0h1
Revises: b5c6d7e8f9g0
Create Date: 2026-02-26

Gap analysis implementation — Phase 1 (Critical) + Phase 2 (Important):

Phase 1:
- К-1: Add GameStatus enum + status column on games
- К-2: Add is_active, is_hidden on player_teams
- К-3: Add is_current on seasons

Phase 2:
- В-1: Add broadcast fields (where_broadcast, video_review_url) on games
- В-2: Add is_featured on games
- В-3: Add missed_penalty, second_yellow to GameEventType enum
- В-4: Add nickname fields on players
- В-5: Add bio fields on players
- В-6: Add genius_id, vsporte_id on players
- В-7: Add photo_url on player_teams
- В-9: Add note on score_table
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c6d7e8f9g0h1'
down_revision: Union[str, None] = 'b5c6d7e8f9g0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── К-1: GameStatus enum + status column on games ──
    gamestatus_enum = sa.Enum(
        'created', 'live', 'finished', 'postponed', 'cancelled', 'technical_defeat',
        name='gamestatus',
    )
    gamestatus_enum.create(op.get_bind(), checkfirst=True)

    op.add_column('games', sa.Column(
        'status', gamestatus_enum,
        nullable=False, server_default='created',
    ))

    # ── В-2: is_featured on games ──
    op.add_column('games', sa.Column(
        'is_featured', sa.Boolean(),
        nullable=False, server_default='false',
    ))

    # ── В-1: Broadcast fields on games ──
    op.add_column('games', sa.Column('where_broadcast', sa.String(500), nullable=True))
    op.add_column('games', sa.Column('video_review_url', sa.String(500), nullable=True))

    # ── К-2: is_active, is_hidden on player_teams ──
    op.add_column('player_teams', sa.Column(
        'is_active', sa.Boolean(),
        nullable=False, server_default='true',
    ))
    op.add_column('player_teams', sa.Column(
        'is_hidden', sa.Boolean(),
        nullable=False, server_default='false',
    ))

    # ── В-7: photo_url on player_teams ──
    op.add_column('player_teams', sa.Column('photo_url', sa.String(), nullable=True))

    # ── К-3: is_current on seasons ──
    op.add_column('seasons', sa.Column(
        'is_current', sa.Boolean(),
        nullable=False, server_default='false',
    ))

    # ── В-3: Add new values to GameEventType enum ──
    # PostgreSQL ALTER TYPE ... ADD VALUE
    op.execute("ALTER TYPE gameeventtype ADD VALUE IF NOT EXISTS 'missed_penalty'")
    op.execute("ALTER TYPE gameeventtype ADD VALUE IF NOT EXISTS 'second_yellow'")

    # ── В-4: Nickname fields on players ──
    op.add_column('players', sa.Column('nickname', sa.String(100), nullable=True))
    op.add_column('players', sa.Column('nickname_kz', sa.String(100), nullable=True))
    op.add_column('players', sa.Column('nickname_en', sa.String(100), nullable=True))

    # ── В-5: Bio fields on players ──
    op.add_column('players', sa.Column('bio', sa.Text(), nullable=True))
    op.add_column('players', sa.Column('bio_kz', sa.Text(), nullable=True))
    op.add_column('players', sa.Column('bio_en', sa.Text(), nullable=True))

    # ── В-6: External IDs on players ──
    op.add_column('players', sa.Column('genius_id', sa.String(100), nullable=True))
    op.add_column('players', sa.Column('vsporte_id', sa.String(100), nullable=True))
    op.create_index('ix_players_genius_id', 'players', ['genius_id'])
    op.create_index('ix_players_vsporte_id', 'players', ['vsporte_id'])

    # ── В-9: Note on score_table ──
    op.add_column('score_table', sa.Column('note', sa.Text(), nullable=True))

    # ── Data migration: set GameStatus from existing data ──
    # Games that are currently live
    op.execute("""
        UPDATE games SET status = 'live'
        WHERE is_live = true AND status = 'created'
    """)
    # Games with scores → finished
    op.execute("""
        UPDATE games SET status = 'finished'
        WHERE home_score IS NOT NULL
          AND away_score IS NOT NULL
          AND is_live = false
          AND status = 'created'
    """)
    # Games with is_technical → technical_defeat
    op.execute("""
        UPDATE games SET status = 'technical_defeat'
        WHERE is_technical = true
          AND home_score IS NOT NULL
          AND away_score IS NOT NULL
          AND status = 'finished'
    """)


def downgrade() -> None:
    # ── В-9 ──
    op.drop_column('score_table', 'note')

    # ── В-6 ──
    op.drop_index('ix_players_vsporte_id', table_name='players')
    op.drop_index('ix_players_genius_id', table_name='players')
    op.drop_column('players', 'vsporte_id')
    op.drop_column('players', 'genius_id')

    # ── В-5 ──
    op.drop_column('players', 'bio_en')
    op.drop_column('players', 'bio_kz')
    op.drop_column('players', 'bio')

    # ── В-4 ──
    op.drop_column('players', 'nickname_en')
    op.drop_column('players', 'nickname_kz')
    op.drop_column('players', 'nickname')

    # Note: Cannot remove enum values from PostgreSQL enums (В-3)

    # ── К-3 ──
    op.drop_column('seasons', 'is_current')

    # ── В-7 ──
    op.drop_column('player_teams', 'photo_url')

    # ── К-2 ──
    op.drop_column('player_teams', 'is_hidden')
    op.drop_column('player_teams', 'is_active')

    # ── В-1 ──
    op.drop_column('games', 'video_review_url')
    op.drop_column('games', 'where_broadcast')

    # ── В-2 ──
    op.drop_column('games', 'is_featured')

    # ── К-1 ──
    op.drop_column('games', 'status')
    sa.Enum(name='gamestatus').drop(op.get_bind(), checkfirst=True)
