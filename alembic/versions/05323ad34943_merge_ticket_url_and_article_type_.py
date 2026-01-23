"""merge_ticket_url_and_article_type_branches

Revision ID: 05323ad34943
Revises: 0a1a327a4a9e, n2i3j4k5l6m7
Create Date: 2026-01-20 09:42:13.521508

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '05323ad34943'
down_revision: Union[str, None] = ('0a1a327a4a9e', 'n2i3j4k5l6m7')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
