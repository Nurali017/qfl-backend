"""merge heads

Revision ID: 11d0da07daf4
Revises: f0e1d2c3b4a5, z4a5b6c7d8e9
Create Date: 2026-02-27 22:01:55.251549

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '11d0da07daf4'
down_revision: Union[str, None] = ('f0e1d2c3b4a5', 'z4a5b6c7d8e9')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
