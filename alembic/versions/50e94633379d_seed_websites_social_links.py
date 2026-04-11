"""seed_websites_social_links

Revision ID: 50e94633379d
Revises: 60f5512d7e7e
Create Date: 2026-04-09 22:52:22.226247

"""
from typing import Sequence, Union

import json
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '50e94633379d'
down_revision: Union[str, None] = '60f5512d7e7e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# team_id → website
TEAM_WEBSITES = {
    90: "https://fctobol.kz",
    91: "https://fcastana.kz/ru",
    13: "https://fckairat.com",
    93: "https://fcelimai.kz",
    51: "https://fc-aktobe.kz/ru",
    94: "https://kaysar.kz",
    81: "https://fcordabasy.kz",
    92: "https://jenisfk.kz",
    318: "https://fcoqjetpes.kz/kk",
    293: "https://fculytau.kz/ru",
    87: "https://qyzyljarfc.kz",
    49: "https://rfcatyrau.kz",
    47: "https://caspiyfc.kz",
    295: "https://altai-fc.kz/ru/",
    # 595 Ертіс — no website
    45: "https://fcjetisu.kz",
}

# club_id → social_links {instagram, telegram}
CLUB_SOCIALS = {
    3:  {"instagram": "https://instagram.com/fca.astana"},
    11: {"instagram": "https://instagram.com/f.c.kairat", "telegram": "https://t.me/fc_kairat"},
    16: {"instagram": "https://instagram.com/fckaysar_official", "telegram": "https://t.me/fckaysar_official"},
    4:  {"instagram": "https://instagram.com/fctobol"},
    50: {"instagram": "https://instagram.com/fc_elimai", "telegram": "https://t.me/fc_elimai"},
    43: {"instagram": "https://instagram.com/jenisfk.kz", "telegram": "https://t.me/jenisfk"},
    10: {"instagram": "https://instagram.com/fcokzhetpes", "telegram": "https://t.me/fcokzhetpes"},
    7:  {"instagram": "https://instagram.com/fc__ordabasy", "telegram": "https://t.me/ordabasyfc"},
    9:  {"instagram": "https://instagram.com/ertis_fk", "telegram": "https://t.me/ertis_fk"},
    13: {"instagram": "https://instagram.com/caspiy_fc", "telegram": "https://t.me/caspiyfc"},
    31: {"instagram": "https://instagram.com/f.c.altai"},
    12: {"instagram": "https://instagram.com/fcqyzyljar", "telegram": "https://t.me/qyzyljartg"},
    1:  {"instagram": "https://instagram.com/fc_atyrau"},
    5:  {"instagram": "https://instagram.com/fcaktobe_official", "telegram": "https://t.me/fc_aktobe"},
    48: {"instagram": "https://instagram.com/fc_ulytau"},
    6:  {"instagram": "https://instagram.com/jetisufk"},
}


def upgrade() -> None:
    for team_id, website in TEAM_WEBSITES.items():
        escaped = website.replace("'", "''")
        op.execute(
            f"UPDATE teams SET website = '{escaped}' WHERE id = {team_id}"
        )

    for club_id, socials in CLUB_SOCIALS.items():
        js = json.dumps(socials).replace("'", "''")
        op.execute(
            f"UPDATE clubs SET social_links = '{js}'::jsonb WHERE id = {club_id}"
        )


def downgrade() -> None:
    team_ids = ','.join(str(t) for t in TEAM_WEBSITES)
    op.execute(f"UPDATE teams SET website = NULL WHERE id IN ({team_ids})")

    club_ids = ','.join(str(c) for c in CLUB_SOCIALS)
    op.execute(f"UPDATE clubs SET social_links = NULL WHERE id IN ({club_ids})")
