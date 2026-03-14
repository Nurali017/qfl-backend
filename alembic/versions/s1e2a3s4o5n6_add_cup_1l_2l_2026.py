"""Add Cup 2026, 1st League 2026, 2nd League 2026 seasons with calendars.

Revision ID: s1e2a3s4o5n6
Revises: zb1g2h3i4j5k6
Create Date: 2026-03-14 12:00:00.000000
"""

from __future__ import annotations

import re
from datetime import date, time

from alembic import op
import sqlalchemy as sa

revision = "s1e2a3s4o5n6"
down_revision = "zb1g2h3i4j5k6"
branch_labels = None
depends_on = None

# ---------------------------------------------------------------------------
# Season IDs
# ---------------------------------------------------------------------------
CUP_SEASON_ID = 202
LEAGUE2_SEASON_ID = 203
LEAGUE1_SEASON_ID = 204

# ---------------------------------------------------------------------------
# Team name normalisation (reused from t1u2v3w4x5y6)
# ---------------------------------------------------------------------------
_TEAM_TRANSLATION_TABLE = str.maketrans(
    {
        "ё": "е",
        "ә": "а",
        "ғ": "г",
        "қ": "к",
        "ң": "н",
        "ө": "о",
        "ұ": "у",
        "ү": "у",
        "һ": "х",
        "і": "и",
        "й": "и",
    }
)
_WHITESPACE_RE = re.compile(r"\s+")
_PUNCT_RE = re.compile(r"[^\w\s]+", re.UNICODE)

# Mapping from normalised fixture token → list of normalised DB-name substrings
TOKEN_ALIASES: dict[str, tuple[str, ...]] = {
    # PL teams that also appear in cup
    "актобе": ("актобе",),
    "тобыл": ("тобыл",),
    "каисар": ("кайсар",),
    "каират": ("кайрат",),
    "астана": ("астана",),
    "атырау": ("атырау",),
    "женис": ("женис",),
    "жетису": ("жетису", "жетысу"),
    "окжетпес": ("окжетпес",),
    "елимаи": ("елимай",),
    "ордабасы": ("ордабасы",),
    "улытау": ("улытау",),
    "ертис": ("ертис", "иртыш", "ertis"),
    "каспии": ("каспий",),
    "алтаи оскемен": ("алтай оскемен", "алтай"),
    "qyzyljar": ("qyzyljar", "кызылжар"),
    # Cup-only teams
    "талас": ("талас",),
    "ансат": ("ансат", "ангсат"),
    "академия онтустик": ("академия",),
    "жаиык": ("жайык", "жаиык"),
    "bks": ("bks",),
    "каршыга": ("каршыга",),
    "жас кыран": ("жас кыран",),
    "туран": ("туран",),
    "sd family": ("sd family",),
    "zhelayev nan": ("zhelayev",),
    "хромтау": ("хромтау",),
    "арыс": ("арыс",),
    "екибастуз": ("екибастуз",),
    "шахтер": ("шахтер",),
    "хан танири": ("хан танири", "хан тангири"),
    "тараз": ("тараз",),
    # 1L specific
    "астана м": ("астана м",),
    "актобе м": ("актобе м",),
    "каират жастар": ("кайрат жастар", "каират жастар"),
    "каспии м": ("каспий м",),
    "тобыл м": ("тобыл м",),
    "елимаи м": ("елимай м",),
    "хан танири м": ("хан танири м", "хан тангири м"),
    # 2L SW
    "туран м": ("туран м",),
    "каршыга м": ("каршыга м",),
    "хромтау м": ("хромтау м",),
    "атырау м": ("атырау м",),
    "ордабасы м": ("ордабасы м",),
    "хан танири м": ("хан танири м", "хан тангири м"),
    "талас м": ("талас м",),
    "каисар м": ("кайсар м",),
    "тараз м": ("тараз м",),
    "жас кыран м": ("жас кыран м",),
    # 2L NE
    "алтаи оскемен м": ("алтай оскемен м", "алтай м"),
    "qyzylzhar м": ("qyzylzhar м", "кызылжар м"),
    "ансат": ("ансат",),
    "жетису м": ("жетису м",),
    "окжетпес м": ("окжетпес м",),
    "шахтер м": ("шахтер м", "шахтёр м"),
    "улытау м": ("улытау м",),
    "ертис павлодар м": ("ертис павлодар м", "ертис м"),
    "sd family": ("sd family",),
    "женис м": ("женис м",),
}

# ---------------------------------------------------------------------------
# Cup 2026 — 1/16 finals (16 matches)
# ---------------------------------------------------------------------------
CUP_MATCHES = [
    # (home, away, date, time_str)
    ("Талас", "Қайсар", date(2026, 4, 8), time(14, 0)),
    ("Аңсат", "Тобыл", date(2026, 4, 8), time(14, 0)),
    ("Академия Оңтүстік", "Ордабасы", date(2026, 4, 8), time(15, 0)),
    ("Жайық", "Қайрат", date(2026, 4, 8), time(15, 0)),
    ("BKS", "Астана", date(2026, 4, 8), time(15, 0)),
    ("Қаршыға", "Жетісу", date(2026, 4, 8), time(15, 0)),
    ("Жас Қыран", "Атырау", date(2026, 4, 8), time(16, 0)),
    ("Тұран", "Елімай", date(2026, 4, 8), time(18, 0)),
    ("SD Family", "Ертіс", date(2026, 4, 9), time(14, 0)),
    ("Zhelayev Nan", "Алтай Өскемен", date(2026, 4, 9), time(14, 0)),
    ("Хромтау", "Жеңіс", date(2026, 4, 9), time(14, 0)),
    ("Арыс", "Ақтобе", date(2026, 4, 9), time(15, 0)),
    ("Екібастұз", "Каспий", date(2026, 4, 9), time(15, 0)),
    ("Шахтер", "Оқжетпес", date(2026, 4, 9), time(15, 0)),
    ("Хан-Тәңірі", "Qyzyljar", date(2026, 4, 9), time(16, 0)),
    ("Тараз", "Ұлытау", date(2026, 4, 9), time(17, 0)),
]

# Cup participants (all unique team names from CUP_MATCHES)
CUP_TEAMS = [
    "Талас", "Қайсар", "Аңсат", "Тобыл", "Академия Оңтүстік", "Ордабасы",
    "Жайық", "Қайрат", "BKS", "Астана", "Қаршыға", "Жетісу",
    "Жас Қыран", "Атырау", "Тұран", "Елімай",
    "SD Family", "Ертіс", "Zhelayev Nan", "Алтай Өскемен",
    "Хромтау", "Жеңіс", "Арыс", "Ақтобе",
    "Екібастұз", "Каспий", "Шахтер", "Оқжетпес",
    "Хан-Тәңірі", "Qyzyljar", "Тараз", "Ұлытау",
]

# ---------------------------------------------------------------------------
# 1st League 2026 — 26 tours × 7 matches = 182 games
# ---------------------------------------------------------------------------
L1_TEAMS = [
    "Арыс", "Жайық", "Астана М", "Актобе М", "Кайрат-Жастар", "Шахтер",
    "Хан-Тәңірі", "Каспий М", "Тараз", "Екібастұз", "Тобыл М",
    "Елімай М", "Тұран", "Академия Оңтүстік",
]

L1_TOUR_DATES = {
    1: date(2026, 4, 2), 2: date(2026, 4, 16), 3: date(2026, 4, 23),
    4: date(2026, 4, 30), 5: date(2026, 5, 7), 6: date(2026, 5, 14),
    7: date(2026, 5, 21), 8: date(2026, 5, 26), 9: date(2026, 6, 11),
    10: date(2026, 6, 18), 11: date(2026, 6, 25), 12: date(2026, 7, 2),
    13: date(2026, 7, 9), 14: date(2026, 7, 16), 15: date(2026, 7, 23),
    16: date(2026, 7, 30), 17: date(2026, 8, 6), 18: date(2026, 8, 13),
    19: date(2026, 8, 20), 20: date(2026, 8, 27), 21: date(2026, 9, 3),
    22: date(2026, 9, 10), 23: date(2026, 9, 17), 24: date(2026, 10, 8),
    25: date(2026, 10, 15), 26: date(2026, 10, 22),
}

# Tours 1-4: per-match date+time from XLSX
L1_DETAILED_MATCHES: dict[int, list[tuple[str, str, date, time]]] = {
    1: [
        ("Арыс", "Жайық", date(2026, 4, 2), time(15, 0)),
        ("Тұран", "Академия Оңтүстік", date(2026, 4, 2), time(18, 0)),
        ("Хан-Тәңірі", "Каспий М", date(2026, 4, 2), time(15, 0)),
        ("Астана М", "Актобе М", date(2026, 4, 3), time(17, 0)),
        ("Тараз", "Екібастұз", date(2026, 4, 3), time(18, 0)),
        ("Тобыл М", "Елімай М", date(2026, 4, 3), time(16, 0)),
        ("Кайрат-Жастар", "Шахтер", date(2026, 4, 3), time(15, 0)),
    ],
    2: [
        ("Каспий М", "Кайрат-Жастар", date(2026, 4, 16), time(15, 0)),
        ("Жайық", "Астана М", date(2026, 4, 16), time(16, 0)),
        ("Тұран", "Арыс", date(2026, 4, 16), time(18, 0)),
        ("Елімай М", "Хан-Тәңірі", date(2026, 4, 17), time(15, 0)),
        ("Академия Оңтүстік", "Тобыл М", date(2026, 4, 17), time(16, 0)),
        ("Шахтер", "Екібастұз", date(2026, 4, 17), time(15, 0)),
        ("Актобе М", "Тараз", date(2026, 4, 17), time(15, 0)),
    ],
    3: [
        ("Арыс", "Шахтер", date(2026, 4, 23), time(15, 0)),
        ("Астана М", "Академия Оңтүстік", date(2026, 4, 23), time(16, 0)),
        ("Екібастұз", "Каспий М", date(2026, 4, 23), time(15, 0)),
        ("Кайрат-Жастар", "Актобе М", date(2026, 4, 23), time(15, 0)),
        ("Хан-Тәңірі", "Жайық", date(2026, 4, 24), time(16, 0)),
        ("Тараз", "Елімай М", date(2026, 4, 24), time(18, 0)),
        ("Тобыл М", "Тұран", date(2026, 4, 24), time(15, 0)),
    ],
    4: [
        ("Актобе М", "Каспий М", date(2026, 4, 30), time(15, 0)),
        ("Арыс", "Астана М", date(2026, 4, 30), time(16, 0)),
        ("Елімай М", "Екібастұз", date(2026, 4, 30), time(15, 0)),
        ("Академия Оңтүстік", "Тараз", date(2026, 5, 1), time(16, 0)),
        ("Шахтер", "Тобыл М", date(2026, 5, 1), time(15, 0)),
        ("Тұран", "Хан-Тәңірі", date(2026, 5, 1), time(18, 0)),
        ("Жайық", "Кайрат-Жастар", date(2026, 5, 1), time(16, 0)),
    ],
}

# Tours 5-26: matchups only, date from L1_TOUR_DATES, no time
L1_FIXTURES: dict[int, list[tuple[str, str]]] = {
    5: [("Астана М", "Тұран"), ("Каспий М", "Шахтер"), ("Екібастұз", "Актобе М"),
        ("Кайрат-Жастар", "Елімай М"), ("Хан-Тәңірі", "Академия Оңтүстік"), ("Тараз", "Жайық"),
        ("Тобыл М", "Арыс")],
    6: [("Арыс", "Хан-Тәңірі"), ("Елімай М", "Каспий М"), ("Академия Оңтүстік", "Кайрат-Жастар"),
        ("Шахтер", "Актобе М"), ("Тобыл М", "Астана М"), ("Тұран", "Тараз"),
        ("Жайық", "Екібастұз")],
    7: [("Актобе М", "Елімай М"), ("Астана М", "Шахтер"), ("Каспий М", "Жайық"),
        ("Екібастұз", "Академия Оңтүстік"), ("Кайрат-Жастар", "Тұран"), ("Хан-Тәңірі", "Тобыл М"),
        ("Тараз", "Арыс")],
    8: [("Арыс", "Кайрат-Жастар"), ("Астана М", "Хан-Тәңірі"), ("Академия Оңтүстік", "Каспий М"),
        ("Шахтер", "Елімай М"), ("Тобыл М", "Тараз"), ("Тұран", "Екібастұз"),
        ("Жайық", "Актобе М")],
    9: [("Актобе М", "Академия Оңтүстік"), ("Каспий М", "Тұран"), ("Екібастұз", "Арыс"),
        ("Елімай М", "Жайық"), ("Кайрат-Жастар", "Тобыл М"), ("Хан-Тәңірі", "Шахтер"),
        ("Тараз", "Астана М")],
    10: [("Арыс", "Каспий М"), ("Астана М", "Кайрат-Жастар"), ("Хан-Тәңірі", "Тараз"),
         ("Академия Оңтүстік", "Елімай М"), ("Шахтер", "Жайық"), ("Тобыл М", "Екібастұз"),
         ("Тұран", "Актобе М")],
    11: [("Актобе М", "Арыс"), ("Каспий М", "Тобыл М"), ("Екібастұз", "Астана М"),
         ("Елімай М", "Тұран"), ("Кайрат-Жастар", "Хан-Тәңірі"), ("Тараз", "Шахтер"),
         ("Жайық", "Академия Оңтүстік")],
    12: [("Арыс", "Елімай М"), ("Астана М", "Каспий М"), ("Кайрат-Жастар", "Тараз"),
         ("Хан-Тәңірі", "Екібастұз"), ("Академия Оңтүстік", "Шахтер"), ("Тобыл М", "Актобе М"),
         ("Тұран", "Жайық")],
    13: [("Актобе М", "Хан-Тәңірі"), ("Каспий М", "Тараз"), ("Екібастұз", "Кайрат-Жастар"),
         ("Елімай М", "Астана М"), ("Академия Оңтүстік", "Арыс"), ("Шахтер", "Тұран"),
         ("Жайық", "Тобыл М")],
    # Second round (tours 14-26)
    14: [("Арыс", "Тұран"), ("Астана М", "Жайық"), ("Екібастұз", "Шахтер"),
         ("Кайрат-Жастар", "Каспий М"), ("Хан-Тәңірі", "Елімай М"), ("Тараз", "Актобе М"),
         ("Тобыл М", "Академия Оңтүстік")],
    15: [("Актобе М", "Кайрат-Жастар"), ("Каспий М", "Екібастұз"), ("Елімай М", "Тараз"),
         ("Академия Оңтүстік", "Астана М"), ("Шахтер", "Арыс"), ("Тұран", "Тобыл М"),
         ("Жайық", "Хан-Тәңірі")],
    16: [("Астана М", "Арыс"), ("Каспий М", "Актобе М"), ("Екібастұз", "Елімай М"),
         ("Кайрат-Жастар", "Жайық"), ("Хан-Тәңірі", "Тұран"), ("Тараз", "Академия Оңтүстік"),
         ("Тобыл М", "Шахтер")],
    17: [("Актобе М", "Екібастұз"), ("Арыс", "Тобыл М"), ("Елімай М", "Кайрат-Жастар"),
         ("Академия Оңтүстік", "Хан-Тәңірі"), ("Шахтер", "Каспий М"), ("Тұран", "Астана М"),
         ("Жайық", "Тараз")],
    18: [("Актобе М", "Шахтер"), ("Астана М", "Тобыл М"), ("Каспий М", "Елімай М"),
         ("Екібастұз", "Жайық"), ("Кайрат-Жастар", "Академия Оңтүстік"), ("Хан-Тәңірі", "Арыс"),
         ("Тараз", "Тұран")],
    19: [("Арыс", "Тараз"), ("Елімай М", "Актобе М"), ("Академия Оңтүстік", "Екібастұз"),
         ("Шахтер", "Астана М"), ("Тобыл М", "Хан-Тәңірі"), ("Тұран", "Кайрат-Жастар"),
         ("Жайық", "Каспий М")],
    20: [("Актобе М", "Жайық"), ("Каспий М", "Академия Оңтүстік"), ("Екібастұз", "Тұран"),
         ("Елімай М", "Шахтер"), ("Кайрат-Жастар", "Арыс"), ("Хан-Тәңірі", "Астана М"),
         ("Тараз", "Тобыл М")],
    21: [("Арыс", "Екібастұз"), ("Астана М", "Тараз"), ("Академия Оңтүстік", "Актобе М"),
         ("Шахтер", "Хан-Тәңірі"), ("Тобыл М", "Кайрат-Жастар"), ("Тұран", "Каспий М"),
         ("Жайық", "Елімай М")],
    22: [("Актобе М", "Тұран"), ("Каспий М", "Арыс"), ("Екібастұз", "Тобыл М"),
         ("Елімай М", "Академия Оңтүстік"), ("Кайрат-Жастар", "Астана М"), ("Тараз", "Хан-Тәңірі"),
         ("Жайық", "Шахтер")],
    23: [("Арыс", "Актобе М"), ("Астана М", "Екібастұз"), ("Хан-Тәңірі", "Кайрат-Жастар"),
         ("Академия Оңтүстік", "Жайық"), ("Шахтер", "Тараз"), ("Тобыл М", "Каспий М"),
         ("Тұран", "Елімай М")],
    24: [("Актобе М", "Тобыл М"), ("Каспий М", "Астана М"), ("Екібастұз", "Хан-Тәңірі"),
         ("Елімай М", "Арыс"), ("Шахтер", "Академия Оңтүстік"), ("Тараз", "Кайрат-Жастар"),
         ("Жайық", "Тұран")],
    25: [("Арыс", "Академия Оңтүстік"), ("Астана М", "Елімай М"), ("Кайрат-Жастар", "Екібастұз"),
         ("Хан-Тәңірі", "Актобе М"), ("Тараз", "Каспий М"), ("Тобыл М", "Жайық"),
         ("Тұран", "Шахтер")],
    26: [("Актобе М", "Астана М"), ("Каспий М", "Хан-Тәңірі"), ("Екібастұз", "Тараз"),
         ("Елімай М", "Тобыл М"), ("Академия Оңтүстік", "Тұран"), ("Шахтер", "Кайрат-Жастар"),
         ("Жайық", "Арыс")],
}

# ---------------------------------------------------------------------------
# 2nd League 2026 — Group A (SW) — 10 teams, 27 tours
# ---------------------------------------------------------------------------
SW_TEAMS = [
    "Туран М", "Қаршыға", "Хромтау", "Атырау М", "Ордабасы М",
    "Хан-Тәңірі М", "Талас", "Қайсар М", "Тараз М", "Жас Қыран",
]

SW_TOUR_DATES = {
    1: date(2026, 4, 1), 2: date(2026, 4, 14), 3: date(2026, 4, 21),
    4: date(2026, 4, 28), 5: date(2026, 5, 5), 6: date(2026, 5, 12),
    7: date(2026, 5, 19), 8: date(2026, 5, 26), 9: date(2026, 6, 10),
    10: date(2026, 6, 16), 11: date(2026, 6, 23), 12: date(2026, 6, 30),
    13: date(2026, 7, 7), 14: date(2026, 7, 14), 15: date(2026, 7, 21),
    16: date(2026, 7, 28), 17: date(2026, 8, 4), 18: date(2026, 8, 11),
    19: date(2026, 8, 18), 20: date(2026, 8, 25), 21: date(2026, 9, 1),
    22: date(2026, 9, 8), 23: date(2026, 9, 15), 24: date(2026, 10, 7),
    25: date(2026, 10, 13), 26: date(2026, 10, 20), 27: date(2026, 10, 27),
}

SW_FIXTURES: dict[int, list[tuple[str, str]]] = {
    1: [("Туран М", "Қаршыға"), ("Хромтау", "Атырау М"), ("Ордабасы М", "Хан-Тәңірі М"),
        ("Талас", "Қайсар М"), ("Тараз М", "Жас Қыран")],
    2: [("Туран М", "Жас Қыран"), ("Атырау М", "Талас"), ("Қайсар М", "Ордабасы М"),
        ("Қаршыға", "Тараз М"), ("Хан-Тәңірі М", "Хромтау")],
    3: [("Туран М", "Хан-Тәңірі М"), ("Қаршыға", "Қайсар М"), ("Ордабасы М", "Хромтау"),
        ("Тараз М", "Атырау М"), ("Жас Қыран", "Талас")],
    4: [("Қайсар М", "Туран М"), ("Атырау М", "Қаршыға"), ("Хан-Тәңірі М", "Тараз М"),
        ("Хромтау", "Жас Қыран"), ("Талас", "Ордабасы М")],
    5: [("Туран М", "Ордабасы М"), ("Қайсар М", "Атырау М"), ("Қаршыға", "Хромтау"),
        ("Тараз М", "Талас"), ("Жас Қыран", "Хан-Тәңірі М")],
    6: [("Хромтау", "Туран М"), ("Атырау М", "Жас Қыран"), ("Хан-Тәңірі М", "Қайсар М"),
        ("Ордабасы М", "Тараз М"), ("Талас", "Қаршыға")],
    7: [("Туран М", "Атырау М"), ("Қайсар М", "Тараз М"), ("Қаршыға", "Хан-Тәңірі М"),
        ("Хромтау", "Талас"), ("Жас Қыран", "Ордабасы М")],
    8: [("Талас", "Туран М"), ("Хан-Тәңірі М", "Атырау М"), ("Ордабасы М", "Қаршыға"),
        ("Тараз М", "Хромтау"), ("Жас Қыран", "Қайсар М")],
    9: [("Тараз М", "Туран М"), ("Қаршыға", "Жас Қыран"), ("Хромтау", "Қайсар М"),
        ("Ордабасы М", "Атырау М"), ("Талас", "Хан-Тәңірі М")],
    10: [("Қаршыға", "Туран М"), ("Атырау М", "Хромтау"), ("Қайсар М", "Талас"),
         ("Хан-Тәңірі М", "Ордабасы М"), ("Жас Қыран", "Тараз М")],
    11: [("Жас Қыран", "Туран М"), ("Хромтау", "Хан-Тәңірі М"), ("Ордабасы М", "Қайсар М"),
         ("Талас", "Атырау М"), ("Тараз М", "Қаршыға")],
    12: [("Туран М", "Талас"), ("Атырау М", "Хан-Тәңірі М"), ("Қайсар М", "Жас Қыран"),
         ("Қаршыға", "Ордабасы М"), ("Хромтау", "Тараз М")],
    13: [("Атырау М", "Туран М"), ("Хан-Тәңірі М", "Қаршыға"), ("Ордабасы М", "Жас Қыран"),
         ("Талас", "Хромтау"), ("Тараз М", "Қайсар М")],
    14: [("Туран М", "Хромтау"), ("Қайсар М", "Хан-Тәңірі М"), ("Қаршыға", "Талас"),
         ("Тараз М", "Ордабасы М"), ("Жас Қыран", "Атырау М")],
    15: [("Ордабасы М", "Туран М"), ("Атырау М", "Қайсар М"), ("Хан-Тәңірі М", "Жас Қыран"),
         ("Хромтау", "Қаршыға"), ("Талас", "Тараз М")],
    16: [("Туран М", "Қайсар М"), ("Қаршыға", "Атырау М"), ("Ордабасы М", "Талас"),
         ("Тараз М", "Хан-Тәңірі М"), ("Жас Қыран", "Хромтау")],
    17: [("Хан-Тәңірі М", "Туран М"), ("Атырау М", "Тараз М"), ("Қайсар М", "Қаршыға"),
         ("Хромтау", "Ордабасы М"), ("Талас", "Жас Қыран")],
    18: [("Туран М", "Тараз М"), ("Атырау М", "Ордабасы М"), ("Қайсар М", "Хромтау"),
         ("Хан-Тәңірі М", "Талас"), ("Жас Қыран", "Қаршыға")],
    19: [("Қайсар М", "Туран М"), ("Қаршыға", "Атырау М"), ("Ордабасы М", "Талас"),
         ("Тараз М", "Хан-Тәңірі М"), ("Жас Қыран", "Хромтау")],
    20: [("Туран М", "Хан-Тәңірі М"), ("Атырау М", "Тараз М"), ("Қаршыға", "Қайсар М"),
         ("Хромтау", "Ордабасы М"), ("Талас", "Жас Қыран")],
    21: [("Туран М", "Жас Қыран"), ("Хромтау", "Хан-Тәңірі М"), ("Ордабасы М", "Қайсар М"),
         ("Талас", "Атырау М"), ("Тараз М", "Қаршыға")],
    22: [("Тараз М", "Туран М"), ("Қайсар М", "Хромтау"), ("Хан-Тәңірі М", "Талас"),
         ("Ордабасы М", "Атырау М"), ("Жас Қыран", "Қаршыға")],
    23: [("Талас", "Туран М"), ("Атырау М", "Хан-Тәңірі М"), ("Қайсар М", "Жас Қыран"),
         ("Қаршыға", "Ордабасы М"), ("Хромтау", "Тараз М")],
    24: [("Туран М", "Атырау М"), ("Хан-Тәңірі М", "Қаршыға"), ("Ордабасы М", "Жас Қыран"),
         ("Талас", "Хромтау"), ("Тараз М", "Қайсар М")],
    25: [("Хромтау", "Туран М"), ("Қайсар М", "Хан-Тәңірі М"), ("Қаршыға", "Талас"),
         ("Тараз М", "Ордабасы М"), ("Жас Қыран", "Атырау М")],
    26: [("Туран М", "Ордабасы М"), ("Атырау М", "Қайсар М"), ("Хан-Тәңірі М", "Жас Қыран"),
         ("Хромтау", "Қаршыға"), ("Талас", "Тараз М")],
    27: [("Туран М", "Қаршыға"), ("Атырау М", "Хромтау"), ("Қайсар М", "Талас"),
         ("Хан-Тәңірі М", "Ордабасы М"), ("Жас Қыран", "Тараз М")],
}

# ---------------------------------------------------------------------------
# 2nd League 2026 — Group B (NE) — 10 teams, 27 tours
# ---------------------------------------------------------------------------
NE_TEAMS = [
    "Алтай Өскемен М", "Qyzylzhar М", "Ансат", "Жетісу М", "Оқжетпес М",
    "Шахтёр М", "Ұлытау М", "Ертіс-Павлодар М", "SD Family", "Жеңіс М",
]

NE_TOUR_DATES = {
    1: date(2026, 4, 2), 2: date(2026, 4, 15), 3: date(2026, 4, 22),
    4: date(2026, 4, 29), 5: date(2026, 5, 6), 6: date(2026, 5, 13),
    7: date(2026, 5, 20), 8: date(2026, 5, 27), 9: date(2026, 6, 10),
    10: date(2026, 6, 17), 11: date(2026, 6, 24), 12: date(2026, 7, 1),
    13: date(2026, 7, 8), 14: date(2026, 7, 15), 15: date(2026, 7, 22),
    16: date(2026, 7, 29), 17: date(2026, 8, 5), 18: date(2026, 8, 12),
    19: date(2026, 8, 19), 20: date(2026, 8, 26), 21: date(2026, 9, 2),
    22: date(2026, 9, 9), 23: date(2026, 9, 16), 24: date(2026, 10, 7),
    25: date(2026, 10, 14), 26: date(2026, 10, 21), 27: date(2026, 10, 28),
}

NE_FIXTURES: dict[int, list[tuple[str, str]]] = {
    1: [("Алтай Өскемен М", "Qyzylzhar М"), ("Ансат", "Жетісу М"), ("Оқжетпес М", "Шахтёр М"),
        ("Ұлытау М", "Ертіс-Павлодар М"), ("SD Family", "Жеңіс М")],
    2: [("Алтай Өскемен М", "SD Family"), ("Ертіс-Павлодар М", "Ансат"), ("Qyzylzhar М", "Оқжетпес М"),
        ("Жеңіс М", "Ұлытау М"), ("Жетісу М", "Шахтёр М")],
    3: [("Ансат", "Жеңіс М"), ("Оқжетпес М", "SD Family"), ("Шахтёр М", "Qyzylzhar М"),
        ("Ұлытау М", "Алтай Өскемен М"), ("Жетісу М", "Ертіс-Павлодар М")],
    4: [("Алтай Өскемен М", "Жетісу М"), ("Ертіс-Павлодар М", "Шахтёр М"), ("Qyzylzhar М", "Ансат"),
        ("Жеңіс М", "Оқжетпес М"), ("SD Family", "Ұлытау М")],
    5: [("Ансат", "SD Family"), ("Ертіс-Павлодар М", "Qyzylzhar М"), ("Оқжетпес М", "Алтай Өскемен М"),
        ("Шахтёр М", "Жеңіс М"), ("Жетісу М", "Ұлытау М")],
    6: [("Алтай Өскемен М", "Ансат"), ("Qyzylzhar М", "Жетісу М"), ("Ұлытау М", "Оқжетпес М"),
        ("Жеңіс М", "Ертіс-Павлодар М"), ("SD Family", "Шахтёр М")],
    7: [("Ансат", "Оқжетпес М"), ("Ертіс-Павлодар М", "Алтай Өскемен М"), ("Qyzylzhar М", "SD Family"),
        ("Шахтёр М", "Ұлытау М"), ("Жетісу М", "Жеңіс М")],
    8: [("Алтай Өскемен М", "Шахтёр М"), ("Оқжетпес М", "Жетісу М"), ("Ұлытау М", "Ансат"),
        ("Жеңіс М", "Qyzylzhar М"), ("SD Family", "Ертіс-Павлодар М")],
    9: [("Ертіс-Павлодар М", "Оқжетпес М"), ("Qyzylzhar М", "Ұлытау М"), ("Шахтёр М", "Ансат"),
        ("Жеңіс М", "Алтай Өскемен М"), ("Жетісу М", "SD Family")],
    10: [("Алтай Өскемен М", "Жеңіс М"), ("Ансат", "Шахтёр М"), ("Оқжетпес М", "Ертіс-Павлодар М"),
         ("Ұлытау М", "Qyzylzhar М"), ("SD Family", "Жетісу М")],
    11: [("Ансат", "Ұлытау М"), ("Ертіс-Павлодар М", "SD Family"), ("Qyzylzhar М", "Жеңіс М"),
         ("Шахтёр М", "Алтай Өскемен М"), ("Жетісу М", "Оқжетпес М")],
    12: [("Алтай Өскемен М", "Ертіс-Павлодар М"), ("Оқжетпес М", "Ансат"), ("Ұлытау М", "Шахтёр М"),
         ("Жеңіс М", "Жетісу М"), ("SD Family", "Qyzylzhar М")],
    13: [("Ансат", "Алтай Өскемен М"), ("Ертіс-Павлодар М", "Жеңіс М"), ("Оқжетпес М", "Ұлытау М"),
         ("Шахтёр М", "SD Family"), ("Жетісу М", "Qyzylzhar М")],
    14: [("Алтай Өскемен М", "Оқжетпес М"), ("Qyzylzhar М", "Ертіс-Павлодар М"),
         ("Ұлытау М", "Жетісу М"), ("Жеңіс М", "Шахтёр М"), ("SD Family", "Ансат")],
    15: [("Ансат", "Qyzylzhar М"), ("Оқжетпес М", "Жеңіс М"), ("Шахтёр М", "Ертіс-Павлодар М"),
         ("Ұлытау М", "SD Family"), ("Жетісу М", "Алтай Өскемен М")],
    16: [("Алтай Өскемен М", "Ұлытау М"), ("Ертіс-Павлодар М", "Жетісу М"),
         ("Qyzylzhar М", "Шахтёр М"), ("Жеңіс М", "Ансат"), ("SD Family", "Оқжетпес М")],
    17: [("Ансат", "Ертіс-Павлодар М"), ("Оқжетпес М", "Qyzylzhar М"), ("Шахтёр М", "Жетісу М"),
         ("Ұлытау М", "Жеңіс М"), ("SD Family", "Алтай Өскемен М")],
    18: [("Ертіс-Павлодар М", "Ұлытау М"), ("Qyzylzhar М", "Алтай Өскемен М"),
         ("Шахтёр М", "Оқжетпес М"), ("Жеңіс М", "SD Family"), ("Жетісу М", "Ансат")],
    19: [("Алтай Өскемен М", "Ұлытау М"), ("Ертіс-Павлодар М", "Жетісу М"),
         ("Qyzylzhar М", "Шахтёр М"), ("Жеңіс М", "Ансат"), ("SD Family", "Оқжетпес М")],
    20: [("Ансат", "Qyzylzhar М"), ("Оқжетпес М", "Жеңіс М"), ("Шахтёр М", "Ертіс-Павлодар М"),
         ("Ұлытау М", "SD Family"), ("Жетісу М", "Алтай Өскемен М")],
    21: [("Алтай Өскемен М", "Шахтёр М"), ("Ансат", "Ұлытау М"), ("Ертіс-Павлодар М", "SD Family"),
         ("Qyzylzhar М", "Жеңіс М"), ("Жетісу М", "Оқжетпес М")],
    22: [("Алтай Өскемен М", "Ертіс-Павлодар М"), ("Оқжетпес М", "Ансат"),
         ("Ұлытау М", "Шахтёр М"), ("Жеңіс М", "Жетісу М"), ("SD Family", "Qyzylzhar М")],
    23: [("Ансат", "Алтай Өскемен М"), ("Ертіс-Павлодар М", "Жеңіс М"),
         ("Оқжетпес М", "Ұлытау М"), ("Шахтёр М", "SD Family"), ("Жетісу М", "Qyzylzhar М")],
    24: [("Алтай Өскемен М", "Оқжетпес М"), ("Qyzylzhar М", "Ертіс-Павлодар М"),
         ("Ұлытау М", "Жетісу М"), ("Жеңіс М", "Шахтёр М"), ("SD Family", "Ансат")],
    25: [("Ансат", "Ертіс-Павлодар М"), ("Оқжетпес М", "Qyzylzhar М"),
         ("Шахтёр М", "Жетісу М"), ("Ұлытау М", "Жеңіс М"), ("SD Family", "Алтай Өскемен М")],
    26: [("Ертіс-Павлодар М", "Ұлытау М"), ("Qyzylzhar М", "Алтай Өскемен М"),
         ("Шахтёр М", "Оқжетпес М"), ("Жеңіс М", "SD Family"), ("Жетісу М", "Ансат")],
    27: [("Алтай Өскемен М", "Жеңіс М"), ("Ансат", "Шахтёр М"), ("Оқжетпес М", "Ертіс-Павлодар М"),
         ("Ұлытау М", "Qyzylzhar М"), ("SD Family", "Жетісу М")],
}


# ===========================================================================
# Helpers
# ===========================================================================

def _normalize_name(value: str | None) -> str:
    if not value:
        return ""
    normalized = value.casefold().translate(_TEAM_TRANSLATION_TABLE)
    normalized = _PUNCT_RE.sub(" ", normalized)
    normalized = _WHITESPACE_RE.sub(" ", normalized).strip()
    return normalized


def _build_team_index(bind) -> dict[int, set[str]]:
    """Build {team_id: {normalised name variants}} from ALL teams."""
    rows = bind.execute(
        sa.text("SELECT id, name, name_kz, name_en FROM teams")
    ).mappings().all()
    index: dict[int, set[str]] = {}
    for row in rows:
        tid = int(row["id"])
        names = index.setdefault(tid, set())
        for key in ("name", "name_kz", "name_en"):
            n = _normalize_name(row[key])
            if n:
                names.add(n)
    return index


def _find_team(token: str, team_index: dict[int, set[str]],
               active_team_ids: set[int] | None = None) -> int | None:
    """Try to find a team by normalised token. Returns team_id or None.

    When multiple candidates match, prefer teams that are already active
    (have games or season participations). If still ambiguous, pick the
    lowest id (oldest = canonical record).
    """
    norm = _normalize_name(token)

    def _pick_best(candidates: set[int]) -> int | None:
        if len(candidates) == 1:
            return next(iter(candidates))
        if not candidates:
            return None
        # Prefer active teams (those with existing season participations / games)
        if active_team_ids:
            active = candidates & active_team_ids
            if len(active) == 1:
                return next(iter(active))
            if active:
                candidates = active
        # Fall back to lowest id (the original/canonical team)
        return min(candidates)

    # Exact match on any name
    exact = {tid for tid, names in team_index.items() if norm in names}
    result = _pick_best(exact)
    if result is not None:
        return result

    # Alias-based substring match
    aliases = tuple(
        _normalize_name(a) for a in TOKEN_ALIASES.get(norm, (norm,))
    )
    candidates: set[int] = set()
    for tid, names in team_index.items():
        if any(any(alias in name for alias in aliases) for name in names):
            candidates.add(tid)
    return _pick_best(candidates)


def _build_active_team_ids(bind) -> set[int]:
    """Return team IDs that have season participations or games."""
    rows = bind.execute(sa.text(
        "SELECT DISTINCT team_id FROM season_participants"
    )).scalars().all()
    active = set(int(r) for r in rows)
    for col in ("home_team_id", "away_team_id"):
        rows2 = bind.execute(sa.text(
            f"SELECT DISTINCT {col} FROM games WHERE {col} IS NOT NULL"
        )).scalars().all()
        active.update(int(r) for r in rows2)
    return active


def _find_or_create_team(
    bind, token: str, name_kz: str | None, team_index: dict[int, set[str]],
    cache: dict[str, int], active_team_ids: set[int],
) -> int:
    """Resolve a fixture team name to a DB team_id; create if needed."""
    norm = _normalize_name(token)
    if norm in cache:
        return cache[norm]

    tid = _find_team(token, team_index, active_team_ids)
    if tid is not None:
        cache[norm] = tid
        return tid

    # Create new team
    display_name = token.strip()
    kz = name_kz or display_name
    new_id = bind.execute(
        sa.text(
            "INSERT INTO teams (name, name_kz, updated_at) "
            "VALUES (:name, :name_kz, NOW()) RETURNING id"
        ),
        {"name": display_name, "name_kz": kz},
    ).scalar_one()
    new_id = int(new_id)
    cache[norm] = new_id
    # Update index so subsequent lookups can find it
    team_index[new_id] = {norm, _normalize_name(kz)}
    return new_id


def _ensure_stage(bind, season_id: int, stage_number: int,
                  name: str, name_kz: str) -> int:
    existing = bind.execute(
        sa.text(
            "SELECT id FROM stages "
            "WHERE season_id = :sid AND stage_number = :sn "
            "ORDER BY id LIMIT 1"
        ),
        {"sid": season_id, "sn": stage_number},
    ).scalar_one_or_none()
    if existing is not None:
        return int(existing)
    return int(bind.execute(
        sa.text(
            "INSERT INTO stages (season_id, name, name_kz, stage_number, sort_order) "
            "VALUES (:sid, :name, :name_kz, :sn, :so) RETURNING id"
        ),
        {"sid": season_id, "name": name, "name_kz": name_kz,
         "sn": stage_number, "so": stage_number},
    ).scalar_one())


def _insert_game(bind, season_id: int, stage_id: int, tour: int,
                 home_id: int, away_id: int, game_date: date,
                 game_time: time | None = None,
                 is_tentative: bool = True) -> None:
    bind.execute(
        sa.text("""
            INSERT INTO games (
                season_id, tour, stage_id, home_team_id, away_team_id,
                date, time, has_stats, has_lineup,
                is_schedule_tentative, updated_at
            ) VALUES (
                :season_id, :tour, :stage_id, :home_id, :away_id,
                :date, :time, false, false,
                :tentative, NOW()
            )
        """),
        {
            "season_id": season_id, "tour": tour, "stage_id": stage_id,
            "home_id": home_id, "away_id": away_id,
            "date": game_date, "time": game_time,
            "tentative": is_tentative,
        },
    )


# ===========================================================================
# upgrade / downgrade
# ===========================================================================

def upgrade() -> None:
    bind = op.get_bind()

    # Fix teams_id_seq in case it's behind the actual max id
    bind.execute(sa.text(
        "SELECT setval('teams_id_seq', GREATEST((SELECT MAX(id) FROM teams), 1))"
    ))

    # Build global team index + cache
    team_index = _build_team_index(bind)
    active_team_ids = _build_active_team_ids(bind)
    cache: dict[str, int] = {}

    # -----------------------------------------------------------------------
    # 1. Look up championship_ids from existing reference seasons
    # -----------------------------------------------------------------------
    cup_champ = bind.execute(
        sa.text("SELECT championship_id FROM seasons WHERE id = 71")
    ).scalar_one()
    l1_champ = bind.execute(
        sa.text("SELECT championship_id FROM seasons WHERE id = 85")
    ).scalar_one()
    l2_champ = bind.execute(
        sa.text("SELECT championship_id FROM seasons WHERE id = 80")
    ).scalar_one()

    # -----------------------------------------------------------------------
    # 2. Create seasons
    # -----------------------------------------------------------------------
    # Cup 2026
    bind.execute(sa.text("""
        INSERT INTO seasons (
            id, championship_id, name, name_kz,
            date_start, date_end,
            frontend_code, tournament_type, tournament_format,
            has_table, has_bracket,
            sponsor_name, sponsor_name_kz,
            logo, sort_order, colors,
            current_round, total_rounds,
            sync_enabled, updated_at
        ) VALUES (
            :id, :champ_id, :name, :name_kz,
            '2026-04-08'::date, '2026-10-31'::date,
            'cup', 'cup', 'knockout',
            false, true,
            'ҚАЗАҚСТАН КУБОГЫ', 'ҚАЗАҚСТАН КУБОГЫ',
            '/images/tournaments/cup.png', 3,
            '{"primary": "74 26 43", "primaryLight": "107 45 66", "primaryDark": "53 18 31", "accent": "139 58 85", "accentSoft": "181 102 126"}'::jsonb,
            NULL, NULL,
            false, NOW()
        )
    """), {"id": CUP_SEASON_ID, "champ_id": cup_champ,
           "name": "Кубок РК 2026", "name_kz": "ҚР Кубогы 2026"})

    # 2nd League 2026
    bind.execute(sa.text("""
        INSERT INTO seasons (
            id, championship_id, name, name_kz,
            date_start, date_end,
            frontend_code, tournament_type, tournament_format,
            has_table, has_bracket,
            sponsor_name, sponsor_name_kz,
            logo, sort_order, colors,
            current_round, total_rounds,
            sync_enabled, updated_at
        ) VALUES (
            :id, :champ_id, :name, :name_kz,
            '2026-04-01'::date, '2026-10-28'::date,
            '2l', 'league', 'round_robin',
            true, false,
            'ЕКІНШІ ЛИГА', 'ЕКІНШІ ЛИГА',
            '/images/tournaments/2l.png', 4,
            '{"primary": "168 106 43", "primaryLight": "196 132 61", "primaryDark": "127 79 32", "accent": "212 168 92", "accentSoft": "229 200 138"}'::jsonb,
            NULL, 27,
            false, NOW()
        )
    """), {"id": LEAGUE2_SEASON_ID, "champ_id": l2_champ,
           "name": "Вторая Лига 2026", "name_kz": "Екінші Лига 2026"})

    # 1st League 2026
    bind.execute(sa.text("""
        INSERT INTO seasons (
            id, championship_id, name, name_kz,
            date_start, date_end,
            frontend_code, tournament_type, tournament_format,
            has_table, has_bracket,
            sponsor_name, sponsor_name_kz,
            logo, sort_order, colors,
            current_round, total_rounds,
            sync_enabled, updated_at
        ) VALUES (
            :id, :champ_id, :name, :name_kz,
            '2026-04-02'::date, '2026-10-22'::date,
            '1l', 'league', 'round_robin',
            true, false,
            'БІРІНШІ ЛИГА', 'БІРІНШІ ЛИГА',
            '/images/tournaments/1l.png', 2,
            '{"primary": "61 122 62", "primaryLight": "78 155 79", "primaryDark": "46 94 47", "accent": "123 198 125", "accentSoft": "163 217 164"}'::jsonb,
            NULL, 26,
            false, NOW()
        )
    """), {"id": LEAGUE1_SEASON_ID, "champ_id": l1_champ,
           "name": "Первая Лига 2026", "name_kz": "Бірінші Лига 2026"})

    # -----------------------------------------------------------------------
    # 3. Resolve / create teams
    # -----------------------------------------------------------------------
    # We use the same cache for all three seasons so teams shared across
    # competitions are resolved only once.

    def resolve(name: str, name_kz: str | None = None) -> int:
        return _find_or_create_team(bind, name, name_kz, team_index, cache,
                                    active_team_ids)

    # -----------------------------------------------------------------------
    # 4. CUP 2026 — participants, stage, games
    # -----------------------------------------------------------------------
    cup_team_ids: dict[str, int] = {}
    for t in CUP_TEAMS:
        cup_team_ids[t] = resolve(t)

    for t, tid in cup_team_ids.items():
        bind.execute(
            sa.text(
                "INSERT INTO season_participants (team_id, season_id) "
                "VALUES (:tid, :sid)"
            ),
            {"tid": tid, "sid": CUP_SEASON_ID},
        )

    cup_stage_id = _ensure_stage(
        bind, CUP_SEASON_ID, 1, "1/16 финала", "1/16 финал")

    for home, away, d, t in CUP_MATCHES:
        _insert_game(bind, CUP_SEASON_ID, cup_stage_id, 1,
                      cup_team_ids[home], cup_team_ids[away], d, t,
                      is_tentative=False)

    # -----------------------------------------------------------------------
    # 5. 1st LEAGUE 2026 — participants, stages, games
    # -----------------------------------------------------------------------
    l1_team_ids: dict[str, int] = {}
    for t in L1_TEAMS:
        l1_team_ids[_normalize_name(t)] = resolve(t)

    for tid in l1_team_ids.values():
        bind.execute(
            sa.text(
                "INSERT INTO season_participants (team_id, season_id) "
                "VALUES (:tid, :sid)"
            ),
            {"tid": tid, "sid": LEAGUE1_SEASON_ID},
        )

    # Tours 1-4 with per-match dates and times
    for tour_num, matches in L1_DETAILED_MATCHES.items():
        stage_id = _ensure_stage(
            bind, LEAGUE1_SEASON_ID, tour_num,
            f"Тур {tour_num}", f"{tour_num} тур")
        for home, away, d, t in matches:
            h_id = l1_team_ids[_normalize_name(home)]
            a_id = l1_team_ids[_normalize_name(away)]
            _insert_game(bind, LEAGUE1_SEASON_ID, stage_id, tour_num,
                          h_id, a_id, d, t, is_tentative=False)

    # Tours 5-26 with only tour date
    for tour_num, fixtures in L1_FIXTURES.items():
        if tour_num <= 4:
            continue  # already handled above
        stage_id = _ensure_stage(
            bind, LEAGUE1_SEASON_ID, tour_num,
            f"Тур {tour_num}", f"{tour_num} тур")
        game_date = L1_TOUR_DATES[tour_num]
        for home, away in fixtures:
            h_id = l1_team_ids[_normalize_name(home)]
            a_id = l1_team_ids[_normalize_name(away)]
            _insert_game(bind, LEAGUE1_SEASON_ID, stage_id, tour_num,
                          h_id, a_id, game_date, None, is_tentative=True)

    # -----------------------------------------------------------------------
    # 6. 2nd LEAGUE 2026 — participants, stages, games
    # -----------------------------------------------------------------------
    # Group A (SW)
    sw_team_ids: dict[str, int] = {}
    for t in SW_TEAMS:
        sw_team_ids[_normalize_name(t)] = resolve(t)

    for tid in sw_team_ids.values():
        bind.execute(
            sa.text(
                "INSERT INTO season_participants (team_id, season_id, group_name) "
                "VALUES (:tid, :sid, 'A')"
            ),
            {"tid": tid, "sid": LEAGUE2_SEASON_ID},
        )

    # Group B (NE)
    ne_team_ids: dict[str, int] = {}
    for t in NE_TEAMS:
        ne_team_ids[_normalize_name(t)] = resolve(t)

    for tid in ne_team_ids.values():
        bind.execute(
            sa.text(
                "INSERT INTO season_participants (team_id, season_id, group_name) "
                "VALUES (:tid, :sid, 'B')"
            ),
            {"tid": tid, "sid": LEAGUE2_SEASON_ID},
        )

    # Stages + games for both groups (shared stages per tour)
    for tour_num in range(1, 28):
        stage_id = _ensure_stage(
            bind, LEAGUE2_SEASON_ID, tour_num,
            f"Тур {tour_num}", f"{tour_num} тур")

        # SW group
        sw_date = SW_TOUR_DATES[tour_num]
        for home, away in SW_FIXTURES[tour_num]:
            h_id = sw_team_ids[_normalize_name(home)]
            a_id = sw_team_ids[_normalize_name(away)]
            _insert_game(bind, LEAGUE2_SEASON_ID, stage_id, tour_num,
                          h_id, a_id, sw_date, None, is_tentative=True)

        # NE group
        ne_date = NE_TOUR_DATES[tour_num]
        for home, away in NE_FIXTURES[tour_num]:
            h_id = ne_team_ids[_normalize_name(home)]
            a_id = ne_team_ids[_normalize_name(away)]
            _insert_game(bind, LEAGUE2_SEASON_ID, stage_id, tour_num,
                          h_id, a_id, ne_date, None, is_tentative=True)


def downgrade() -> None:
    bind = op.get_bind()
    for sid in (CUP_SEASON_ID, LEAGUE2_SEASON_ID, LEAGUE1_SEASON_ID):
        bind.execute(
            sa.text("DELETE FROM games WHERE season_id = :sid"), {"sid": sid})
        bind.execute(
            sa.text("DELETE FROM stages WHERE season_id = :sid"), {"sid": sid})
        bind.execute(
            sa.text("DELETE FROM season_participants WHERE season_id = :sid"),
            {"sid": sid})
        bind.execute(
            sa.text("DELETE FROM seasons WHERE id = :sid"), {"sid": sid})

    # Clean up teams created by this migration that have no other references
    bind.execute(sa.text("""
        DELETE FROM teams
        WHERE id NOT IN (SELECT DISTINCT home_team_id FROM games WHERE home_team_id IS NOT NULL)
          AND id NOT IN (SELECT DISTINCT away_team_id FROM games WHERE away_team_id IS NOT NULL)
          AND id NOT IN (SELECT DISTINCT team_id FROM season_participants)
          AND id NOT IN (SELECT DISTINCT team_id FROM player_teams)
    """))
