"""One-time script: map FCMS team IDs for Вторая Лига 2026.

Two FCMS competitions:
  - 3596 (Юго-Запад, group 11083)
  - 3597 (Северо-Восток, group 11084)
Both map to local season 203.

Usage:
    cd backend && python -m scripts.map_fcms_second_league [--dry-run]
"""

import asyncio
import re
import sys

from sqlalchemy import select, update

from app.database import AsyncSessionLocal
from app.models.team import Team
from app.models.player_team import PlayerTeam
from app.models.season_participant import SeasonParticipant
from app.services.fcms_client import FcmsClient

COMPETITIONS = [
    (3596, "Юго-Запад"),
    (3597, "Северо-Восток"),
]
SEASON_ID = 203

_TEAM_TRANSLATION_TABLE = str.maketrans({
    "ё": "е", "ә": "а", "ғ": "г", "қ": "к", "ң": "н",
    "ө": "о", "ұ": "у", "ү": "у", "һ": "х", "і": "и", "й": "и",
})
_WHITESPACE_RE = re.compile(r"\s+")
_PUNCT_RE = re.compile(r"[^\w\s]+", re.UNICODE)

# Known FCMS→local name aliases for Second League teams
ALIASES = {
    "хромтау": "хромтау",
    "hromtau": "хромтау",
    "талас": "талас",
    "talas": "талас",
    "каршыга": "каршыга",
    "karshyga": "каршыга",
    "хан тенгри": "хан танири",
    "khan tengri": "хан танири",
    "жас кыран": "жас кыран",
    "zhas kyran": "жас кыран",
    "алтаи оскемен": "алтаи м",
    "altai oskemen": "алтаи м",
    "кызыл жар": "кызылжар",
    "kyzyl-zhar": "кызылжар",
    "kyzylzhar": "кызылжар",
    "sd family м": "sd family",
    "sd family": "sd family",
    "ансат": "ансат",
    "ansat": "ансат",
    "жетысу": "жетису",
    "zhetysu": "жетису",
    "ертис м": "ертис-павлодар м",
    "ertis": "ертис",
}


def normalize(name: str) -> str:
    s = name.lower().translate(_TEAM_TRANSLATION_TABLE)
    s = _PUNCT_RE.sub(" ", s)
    s = _WHITESPACE_RE.sub(" ", s).strip()
    return s


def match_team(fcms_title: str, fcms_int_title: str, local_teams: list[Team]) -> Team | None:
    """Try to match FCMS team to local team by name similarity."""
    for title in [fcms_title, fcms_int_title]:
        if not title:
            continue
        norm = normalize(title)

        # Direct match
        for t in local_teams:
            for attr in ("name", "name_kz", "name_en"):
                local_name = getattr(t, attr, None)
                if local_name and normalize(local_name) == norm:
                    return t

        # Substring match
        for t in local_teams:
            for attr in ("name", "name_kz", "name_en"):
                local_name = getattr(t, attr, None)
                if not local_name:
                    continue
                local_norm = normalize(local_name)
                if norm in local_norm or local_norm in norm:
                    return t

        # Alias match
        for alias_key, alias_val in ALIASES.items():
            if alias_key in norm or norm in alias_key:
                for t in local_teams:
                    for attr in ("name", "name_kz", "name_en"):
                        local_name = getattr(t, attr, None)
                        if local_name and alias_val in normalize(local_name):
                            return t

    return None


async def main():
    dry_run = "--dry-run" in sys.argv

    client = FcmsClient()
    all_competitors = []
    try:
        for comp_id, conf_name in COMPETITIONS:
            comps = await client.get_competition_competitors(comp_id)
            print(f"FCMS Competition {comp_id} ({conf_name}): {len(comps)} teams")
            all_competitors.extend(comps)
    finally:
        await client.close()

    print(f"\nTotal FCMS teams: {len(all_competitors)}\n")

    async with AsyncSessionLocal() as db:
        # Get local teams from season_participants
        sp_result = await db.execute(
            select(Team)
            .where(Team.id.in_(
                select(SeasonParticipant.team_id).where(SeasonParticipant.season_id == SEASON_ID)
            ))
            .order_by(Team.name)
        )
        local_teams_from_sp = list(sp_result.scalars().all())

        # Also from player_teams
        pt_result = await db.execute(
            select(Team)
            .where(Team.id.in_(
                select(PlayerTeam.team_id).where(PlayerTeam.season_id == SEASON_ID).distinct()
            ))
            .order_by(Team.name)
        )
        local_teams_from_pt = list(pt_result.scalars().all())

        # Merge
        seen_ids = set()
        local_teams = []
        for t in local_teams_from_sp + local_teams_from_pt:
            if t.id not in seen_ids:
                local_teams.append(t)
                seen_ids.add(t.id)

        print(f"Local teams in season {SEASON_ID}: {len(local_teams)}")
        for t in local_teams:
            existing = f" (fcms={t.fcms_team_id})" if t.fcms_team_id else ""
            print(f"  id={t.id}: {t.name}{existing}")

        print(f"\n{'='*60}")
        print("Matching FCMS → Local:")
        print(f"{'='*60}\n")

        sql_lines = []
        matched = 0
        unmatched = []

        for comp in all_competitors:
            fcms_team_id = comp.get("teamId")
            title = comp.get("title", "")
            int_title = comp.get("internationalTitle", "")

            local = match_team(title, int_title, local_teams)
            if local:
                matched += 1
                status = "MATCHED"
                if local.fcms_team_id == fcms_team_id:
                    status = "ALREADY SET"
                elif local.fcms_team_id and local.fcms_team_id != fcms_team_id:
                    status = f"CONFLICT (current={local.fcms_team_id})"
                else:
                    sql = f"UPDATE teams SET fcms_team_id = {fcms_team_id} WHERE id = {local.id};  -- {local.name}"
                    sql_lines.append(sql)

                    if not dry_run:
                        await db.execute(
                            update(Team).where(Team.id == local.id).values(fcms_team_id=fcms_team_id)
                        )

                print(f"  FCMS {fcms_team_id}: {title} ({int_title}) -> {local.name} (id={local.id}) {status}")
            else:
                unmatched.append((fcms_team_id, title, int_title))
                print(f"  FCMS {fcms_team_id}: {title} ({int_title}) -> NOT FOUND")

        if not dry_run:
            await db.commit()
            print(f"\nApplied {len(sql_lines)} mappings to database")
        else:
            print(f"\n[DRY RUN] Would apply {len(sql_lines)} mappings")

        print(f"\nMatched: {matched}/{len(all_competitors)}")
        if unmatched:
            print(f"\nUnmatched ({len(unmatched)}):")
            for tid, title, int_title in unmatched:
                print(f"  FCMS {tid}: {title} / {int_title}")

        if sql_lines:
            print(f"\n{'='*60}")
            print("SQL:")
            print(f"{'='*60}")
            for sql in sql_lines:
                print(sql)


if __name__ == "__main__":
    asyncio.run(main())
