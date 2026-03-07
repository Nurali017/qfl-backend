"""Populate stadium Kazakh translations (name_kz, city_kz)

Stadiums 1-34 are the primary stadiums used in PL games.
They have city (Russian) but city_kz and name_kz are NULL,
causing the frontend to display Russian when UI is set to Kazakh.

Revision ID: d0e1f2g3h4i5
Revises: c9d0e1f2g3h4
Create Date: 2026-03-03 12:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = "d0e1f2g3h4i5"
down_revision = "c9d0e1f2g3h4"
branch_labels = None
depends_on = None

# (id, name_kz, city_kz)
STADIUM_KZ = [
    (1, "Ғ. Мұратбаев атындағы стадион", "Қызылорда"),
    (2, "Қостанай Орталық стадионы", "Қостанай"),
    (3, "«Қарасай» стадионы", "Петропавл"),
    (4, "Орталық стадион (ФК «Ақтөбе»)", "Ақтөбе"),
    (5, "Қажымұқан атындағы Орталық стадион", "Шымкент"),
    (6, "«Оқжетпес»", "Көкшетау"),
    (7, "Түркістан Арена", "Түркістан"),
    (8, "«Спартак» стадионы", "Семей"),
    (9, "«Мұнайшы» спорт кешені", "Атырау"),
    (10, "«Астана Арена» СК", "Астана"),
    (11, "«Металлург»", "Жезқазған"),
    (12, "«Намыс» стадионы", "Шымкент"),
    (13, "Орталық стадион (ФК «Тараз»)", "Тараз"),
    (14, "Жас Қыран", "Алматы"),
    (15, "Каспий - BS Arena", "Ақтау"),
    (16, "«Шахтёр» стадионы", "Қарағанды"),
    (17, "Октябрь Жарылқапов атындағы стадион", "Түркістан"),
    (18, "Т. Сегізбаев атындағы Академия", "Алматы"),
    (19, "ОСДЮСШОР", "Ақтөбе"),
    (20, "«Хан-Тәңірі»", "Алматы"),
    (21, "«Шахтёр» («Екібастұз» ФК)", "Екібастұз"),
    (25, "Ғани Мұратбаев атындағы стадион", "Қызылорда"),
    (27, "«Цементник» стадионы", "Шымкент"),
    (28, "Б. Оңғаров атындағы стадион", "Талдықорған"),
    (29, "ҚФФ Футбол үйі", "Астана"),
    (30, "«Жастар» стадионы", "Петропавл"),
    (31, "«Восток»", "Семей"),
    (33, "Металлург", "Жезқазған"),
    (34, "Орталық стадион", "Алматы"),
    (111, "Қайсар Арена", "Қызылорда"),
]


def upgrade() -> None:
    bind = op.get_bind()
    for stadium_id, name_kz, city_kz in STADIUM_KZ:
        bind.execute(
            sa.text(
                "UPDATE stadiums "
                "SET name_kz = COALESCE(name_kz, :name_kz), "
                "    city_kz = COALESCE(city_kz, :city_kz) "
                "WHERE id = :id"
            ),
            {"id": stadium_id, "name_kz": name_kz, "city_kz": city_kz},
        )


def downgrade() -> None:
    bind = op.get_bind()
    for stadium_id, _, _ in STADIUM_KZ:
        bind.execute(
            sa.text(
                "UPDATE stadiums SET name_kz = NULL, city_kz = NULL WHERE id = :id"
            ),
            {"id": stadium_id},
        )
