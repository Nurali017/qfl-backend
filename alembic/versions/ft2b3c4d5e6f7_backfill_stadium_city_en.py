"""backfill stadium city_en for weather geocoding

Revision ID: ft2b3c4d5e6f7
Revises: ft1a2b3c4d5e6
Create Date: 2026-03-11 13:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
from sqlalchemy import text


# revision identifiers, used by Alembic.
revision: str = "ft2b3c4d5e6f7"
down_revision: Union[str, None] = "ft1a2b3c4d5e6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

CITY_MAP = {
    "Алматы": "Almaty",
    "Астана": "Astana",
    "Шымкент": "Shymkent",
    "Караганда": "Karaganda",
    "Актобе": "Aktobe",
    "Тараз": "Taraz",
    "Павлодар": "Pavlodar",
    "Семей": "Semey",
    "Атырау": "Atyrau",
    "Костанай": "Kostanay",
    "Уральск": "Oral",
    "Петропавловск": "Petropavlovsk",
    "Кокшетау": "Kokshetau",
    "Талдыкорган": "Taldykorgan",
    "Актау": "Aktau",
    "Туркестан": "Turkestan",
    "Экибастуз": "Ekibastuz",
    "Рудный": "Rudny",
    "Жезказган": "Zhezkazgan",
    "Кызылорда": "Kyzylorda",
    "Қызылорда": "Kyzylorda",
    "Талгар": "Talgar",
    "Каратау": "Karatau",
    "Арыс": "Arys",
    "Мырзакент": "Myrzakent",
    "Отеген батыр": "Otegen Batyr",
}


def upgrade() -> None:
    conn = op.get_bind()
    for city_ru, city_en in CITY_MAP.items():
        conn.execute(
            text("UPDATE stadiums SET city_en = :en WHERE city = :ru AND city_en IS NULL"),
            {"en": city_en, "ru": city_ru},
        )


def downgrade() -> None:
    # Not reverting — city_en is useful data
    pass
