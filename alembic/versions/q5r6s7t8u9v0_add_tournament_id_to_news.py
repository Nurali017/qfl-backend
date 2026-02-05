"""Add tournament_id to news

Revision ID: q5r6s7t8u9v0
Revises: p4k5l6m7n8o9
Create Date: 2026-02-05

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'q5r6s7t8u9v0'
down_revision: str = 'p4k5l6m7n8o9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Mapping from category names (cyrillic) to tournament IDs
CATEGORY_TO_TOURNAMENT_ID = {
    'ПРЕМЬЕР-ЛИГА': 'pl',
    'ПЕРВАЯ ЛИГА': '1l',
    'БІРІНШІ ЛИГА': '1l',
    'КУБОК': 'cup',
    'ВТОРАЯ ЛИГА': '2l',
    'ЕКІНШІ ЛИГА': '2l',
    'ЖЕНСКАЯ ЛИГА': 'el',
    'ӘЙЕЛДЕР ЛИГАСЫ': 'el',
}


def upgrade() -> None:
    # Add tournament_id column
    op.add_column('news', sa.Column('tournament_id', sa.String(10), nullable=True))
    op.create_index('ix_news_tournament_id', 'news', ['tournament_id'])
    
    connection = op.get_bind()
    
    # First, try to populate from category field
    for category, tournament_id in CATEGORY_TO_TOURNAMENT_ID.items():
        connection.execute(
            sa.text(
                "UPDATE news SET tournament_id = :tournament_id WHERE UPPER(category) = :category"
            ),
            {'tournament_id': tournament_id, 'category': category}
        )
    
    # Then, populate from excerpt field for records without tournament_id
    # This handles cases where category is generic but excerpt contains specific tournament info
    excerpt_patterns = [
        ('cup', '%КУБОК%'),
        ('1l', '%ПЕРВАЯ ЛИГА%'),
        ('1l', '%БІРІНШІ ЛИГА%'),
        ('2l', '%ВТОРАЯ ЛИГА%'),
        ('2l', '%ЕКІНШІ ЛИГА%'),
        ('el', '%ЖЕНСКАЯ%'),
        ('el', '%ӘЙЕЛДЕР%'),
        ('pl', '%ПРЕМЬЕР%'),
    ]
    
    for tournament_id, pattern in excerpt_patterns:
        connection.execute(
            sa.text(
                "UPDATE news SET tournament_id = :tid WHERE UPPER(excerpt) LIKE :pattern AND tournament_id IS NULL"
            ),
            {'tid': tournament_id, 'pattern': pattern}
        )


def downgrade() -> None:
    op.drop_index('ix_news_tournament_id', table_name='news')
    op.drop_column('news', 'tournament_id')
