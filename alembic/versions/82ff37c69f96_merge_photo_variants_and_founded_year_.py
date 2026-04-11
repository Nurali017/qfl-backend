"""merge photo variants and founded year heads

Revision ID: 82ff37c69f96
Revises: 50e94633379d, zw9x0y1z2a3b4
Create Date: 2026-04-11 09:00:50.523377

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '82ff37c69f96'
down_revision: Union[str, None] = ('50e94633379d', 'zw9x0y1z2a3b4')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
