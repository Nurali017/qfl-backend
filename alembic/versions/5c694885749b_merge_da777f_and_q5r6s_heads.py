"""merge da777f and q5r6s heads

Revision ID: 5c694885749b
Revises: da777f77f896, r6s7t8u9v0w1
Create Date: 2026-02-06 02:24:59.652786

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5c694885749b'
down_revision: Union[str, None] = ('da777f77f896', 'r6s7t8u9v0w1')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
