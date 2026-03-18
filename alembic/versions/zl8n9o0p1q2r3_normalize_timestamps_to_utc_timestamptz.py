"""normalize timestamp columns to UTC timestamptz

Revision ID: zl8n9o0p1q2r3
Revises: zk7m8n9o0p1q2
Create Date: 2026-03-18 23:30:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "zl8n9o0p1q2r3"
down_revision: Union[str, None] = "zk7m8n9o0p1q2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _convert_all_public_timestamps(*, source_type: str, target_type: str) -> None:
    op.execute(
        sa.text(
            f"""
            DO $$
            DECLARE
                column_row RECORD;
            BEGIN
                FOR column_row IN
                    SELECT c.table_schema, c.table_name, c.column_name
                    FROM information_schema.columns c
                    JOIN information_schema.tables t
                      ON t.table_schema = c.table_schema
                     AND t.table_name = c.table_name
                    WHERE c.table_schema = 'public'
                      AND c.data_type = '{source_type}'
                      AND t.table_type = 'BASE TABLE'
                LOOP
                    EXECUTE format(
                        'ALTER TABLE %I.%I ALTER COLUMN %I TYPE {target_type} USING (%I AT TIME ZONE ''UTC'')',
                        column_row.table_schema,
                        column_row.table_name,
                        column_row.column_name,
                        column_row.column_name
                    );
                END LOOP;
            END
            $$;
            """
        )
    )


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    # All legacy naive timestamps were written as UTC by application code.
    # Convert them to explicit timestamptz without shifting the absolute instant.
    _convert_all_public_timestamps(
        source_type="timestamp without time zone",
        target_type="TIMESTAMP WITH TIME ZONE",
    )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    _convert_all_public_timestamps(
        source_type="timestamp with time zone",
        target_type="TIMESTAMP WITHOUT TIME ZONE",
    )
