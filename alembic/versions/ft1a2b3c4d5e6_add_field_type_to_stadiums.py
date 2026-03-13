"""add field_type to stadiums

Revision ID: ft1a2b3c4d5e6
Revises: w1e2a3t4h5e6
Create Date: 2026-03-11 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "ft1a2b3c4d5e6"
down_revision: Union[str, None] = "w1e2a3t4h5e6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

fieldtype_enum = sa.Enum("artificial", "natural", name="fieldtype")


def upgrade() -> None:
    fieldtype_enum.create(op.get_bind(), checkfirst=True)
    op.execute("""
        ALTER TABLE stadiums ADD COLUMN IF NOT EXISTS field_type fieldtype
    """)


def downgrade() -> None:
    op.drop_column("stadiums", "field_type")
    fieldtype_enum.drop(op.get_bind(), checkfirst=True)
