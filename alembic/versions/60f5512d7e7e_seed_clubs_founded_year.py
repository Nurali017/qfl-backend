"""seed_clubs_founded_year

Revision ID: 60f5512d7e7e
Revises: d9e8b6e0b53e
Create Date: 2026-04-09 22:47:59.685250

"""
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = '60f5512d7e7e'
down_revision: Union[str, None] = 'd9e8b6e0b53e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# club_id → founded_year
CLUB_FOUNDED = {
    5: 1967,   # Актобе
    31: 2021,  # Алтай
    3: 2009,   # Астана
    1: 1980,   # Атырау
    50: 1964,  # Елимай
    43: 1951,  # Женис
    6: 1981,   # Жетысу
    9: 1965,   # Иртыш
    11: 1954,  # Кайрат
    16: 1968,  # Кайсар
    13: 1946,  # Каспий
    12: 1968,  # Кызылжар
    10: 1957,  # Окжетпес
    7: 1949,   # Ордабасы
    4: 1967,   # Тобол
    48: 2022,  # Улытау
}

# team_id → stadium_id (main home stadiums)
TEAM_STADIUMS = {
    51: 4,    # Актобе → Центральный (ФК «Актобе»)
    295: 76,  # Алтай → Абай Арена (Семей)
    91: 10,   # Астана → Астана Арена
    49: 9,    # Атырау → Мунайшы
    93: 67,   # Елимай → Центральный Павлодар
    92: 81,   # Женис → Нур-Аман
    45: 39,   # Жетысу → Центральный (ФК «Жетысу»)
    595: 67,  # Иртыш → Центральный Павлодар
    13: 40,   # Кайрат → Центральный (ФК «Кайрат»)
    94: 111,  # Кайсар → Кайсар Арена
    47: 15,   # Каспий → Каспий - BS Arena
    87: 30,   # Кызылжар → Жастар
    318: 6,   # Окжетпес → Окжетпес
    81: 5,    # Ордабасы → Центральный им. Кажымукана
    90: 65,   # Тобол → Тобыл Арена
    293: 11,  # Улытау → Металлург
}


def upgrade() -> None:
    # Seed founded_year on clubs
    for club_id, year in CLUB_FOUNDED.items():
        op.execute(
            f"UPDATE clubs SET founded_year = {year} WHERE id = {club_id}"
        )

    # Link teams to their home stadiums (where not already set)
    for team_id, stadium_id in TEAM_STADIUMS.items():
        op.execute(
            f"UPDATE teams SET stadium_id = {stadium_id} WHERE id = {team_id} AND stadium_id IS NULL"
        )


def downgrade() -> None:
    club_ids = ','.join(str(c) for c in CLUB_FOUNDED)
    op.execute(f"UPDATE clubs SET founded_year = NULL WHERE id IN ({club_ids})")

    team_ids = ','.join(str(t) for t in TEAM_STADIUMS)
    op.execute(f"UPDATE teams SET stadium_id = NULL WHERE id IN ({team_ids})")
