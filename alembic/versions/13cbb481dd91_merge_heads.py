"""merge heads

Revision ID: 13cbb481dd91
Revises: b9c0d1e2f3g4, d0e1f2g3h4i5
Create Date: 2026-03-06 14:18:36.030464

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '13cbb481dd91'
down_revision: Union[str, None] = ('b9c0d1e2f3g4', 'd0e1f2g3h4i5')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
