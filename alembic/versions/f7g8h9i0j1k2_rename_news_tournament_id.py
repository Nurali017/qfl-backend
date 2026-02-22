"""rename_news_tournament_id_to_championship_code

Revision ID: f7g8h9i0j1k2
Revises: e6f7g8h9i0j1
Create Date: 2026-02-22
"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "f7g8h9i0j1k2"
down_revision: Union[str, None] = "e6f7g8h9i0j1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column("news", "tournament_id", new_column_name="championship_code")
    op.execute("ALTER INDEX ix_news_tournament_id RENAME TO ix_news_championship_code")


def downgrade() -> None:
    op.alter_column("news", "championship_code", new_column_name="tournament_id")
    op.execute("ALTER INDEX ix_news_championship_code RENAME TO ix_news_tournament_id")
