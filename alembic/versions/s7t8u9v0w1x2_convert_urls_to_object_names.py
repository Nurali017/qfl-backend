"""Convert absolute MinIO URLs to relative object names.

Revision ID: s7t8u9v0w1x2
Revises: r6s7t8u9v0w1
Create Date: 2026-02-06

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "s7t8u9v0w1x2"
down_revision: Union[str, None] = "r6s7t8u9v0w1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# The bucket name used in all environments
BUCKET = "qfl-files"


def upgrade() -> None:
    # Strip everything up to and including /{bucket}/ from URL fields,
    # leaving only the MinIO object name (e.g. "player_photos/uuid.webp").
    # External URLs (not containing the bucket) are left untouched.
    pattern = f"^.*?/{BUCKET}/"
    replacement = ""

    tables_and_columns = [
        ("teams", "logo_url"),
        ("players", "photo_url"),
        ("coaches", "photo_url"),
        ("referees", "photo_url"),
        ("countries", "flag_url"),
        ("news", "image_url"),
    ]

    conn = op.get_bind()
    for table, column in tables_and_columns:
        conn.execute(
            sa.text(
                f"UPDATE {table} "
                f"SET {column} = regexp_replace({column}, :pattern, :replacement) "
                f"WHERE {column} LIKE :like_pattern"
            ),
            {
                "pattern": pattern,
                "replacement": replacement,
                "like_pattern": f"%/{BUCKET}/%",
            },
        )


def downgrade() -> None:
    # Cannot reliably reconstruct original absolute URLs.
    # The resolve_file_url() function handles both formats gracefully.
    pass
