"""Populate team home stadiums from hardcoded mapping

Revision ID: p4k5l6m7n8o9
Revises: o3j4k5l6m7n8
Create Date: 2026-01-27 00:00:00.000000
"""
from alembic import op

revision = 'p4k5l6m7n8o9'
down_revision = 'o3j4k5l6m7n8'
branch_labels = None
depends_on = None

# KPL 2025 home stadium mapping
TEAM_STADIUMS = {
    13: 34,    # Кайрат -> Центральный стадион
    45: 28,    # Жетысу -> стадион им Б. Онгарова
    49: 9,     # Атырау -> Спорткомплекс «Мунайшы»
    51: 4,     # Актобе -> Центральный (ФК «Актобе»)
    80: 7,     # Туран -> Туркестан Арена
    81: 5,     # Ордабасы -> Центральный стадион им. Кажымукана
    87: 3,     # Кызылжар -> стадион Карасай
    90: 2,     # Тобол -> Центральный стадион Костанай
    91: 10,    # Астана -> СК «Астана Арена»
    92: 11,    # Женис -> «Металлург»
    93: 8,     # Елимай -> Стадион «Спартак»
    94: 1,     # Кайсар -> Стадион имени Г. Муратбаева
    293: 11,   # Улытау -> «Металлург»
    318: 6,    # Окжетпес -> «Окжетпес»
}


def upgrade() -> None:
    for team_id, stadium_id in TEAM_STADIUMS.items():
        op.execute(
            f"UPDATE teams SET stadium_id = {stadium_id} WHERE id = {team_id} AND stadium_id IS NULL"
        )


def downgrade() -> None:
    for team_id in TEAM_STADIUMS:
        op.execute(
            f"UPDATE teams SET stadium_id = NULL WHERE id = {team_id}"
        )
