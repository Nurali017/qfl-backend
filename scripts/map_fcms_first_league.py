"""One-time script: map FCMS team IDs for Первая Лига 2026 (competition 3585).

Fetches competitors from FCMS API, matches them to local teams in season 204
by name similarity, and outputs ready-to-run SQL + applies mappings to DB.

Usage:
    cd backend && python -m scripts.map_fcms_first_league [--dry-run]
"""

import asyncio
import re
import sys

from sqlalchemy import select, update

from app.database import AsyncSessionLocal
from app.models.team import Team
from app.models.player_team import PlayerTeam
from app.services.fcms_client import FcmsClient

COMPETITION_ID = 3585
SEASON_ID = 204

# Same normalization as used in migration scripts
_TEAM_TRANSLATION_TABLE = str.maketrans({
    "ё": "е", "ә": "а", "ғ": "г", "қ": "к", "ң": "н",
    "ө": "о", "ұ": "у", "ү": "у", "һ": "х", "і": "и", "й": "и",
})
_WHITESPACE_RE = re.compile(r"\s+")
_PUNCT_RE = re.compile(r"[^\w\s]+", re.UNICODE)

# Known FCMS→local name aliases
ALIASES = {
    "astana-m": "астана м",
    "aktobe-m": "актобе м",
    "kairat zhastar": "кайрат жастар",
    "kaspiy-m": "каспий м",
    "tobol-m": "тобыл м",
    "elimai-m": "елимай м",
    "khan tangiri": "хан тангири",
    "khan tengri": "хан тангири",
    "academiya": "академия",
    "turan": "туран",
    "taraz": "тараз",
    "shakhter": "шахтер",
    "ekibastuz": "екибастуз",
    "arys": "арыс",
    "zhaiyk": "жайык",
    "академия онтустик": "онтустик",
    "ontustyk": "онтустик",
    "екибастуз": "экибастуз",
    "ekibastuz": "экибастуз",
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
            if normalize(t.name) == norm:
                return t

        # Substring match
        for t in local_teams:
            local_norm = normalize(t.name)
            if norm in local_norm or local_norm in norm:
                return t

        # Alias match
        for alias_key, alias_val in ALIASES.items():
            if alias_key in norm or norm in alias_key:
                for t in local_teams:
                    if alias_val in normalize(t.name):
                        return t

    return None


async def main():
    dry_run = "--dry-run" in sys.argv

    client = FcmsClient()
    try:
        competitors = await client.get_competition_competitors(COMPETITION_ID)
    finally:
        await client.close()

    print(f"\nFCMS Competition {COMPETITION_ID}: {len(competitors)} teams\n")

    async with AsyncSessionLocal() as db:
        # Get local teams participating in season 204
        result = await db.execute(
            select(Team)
            .where(Team.id.in_(
                select(PlayerTeam.team_id).where(PlayerTeam.season_id == SEASON_ID).distinct()
            ))
            .order_by(Team.name)
        )
        local_teams_from_pt = list(result.scalars().all())

        # Also get from season_participants
        from app.models.season_participant import SeasonParticipant
        sp_result = await db.execute(
            select(Team)
            .where(Team.id.in_(
                select(SeasonParticipant.team_id).where(SeasonParticipant.season_id == SEASON_ID)
            ))
            .order_by(Team.name)
        )
        local_teams_from_sp = list(sp_result.scalars().all())

        # Merge
        seen_ids = set()
        local_teams = []
        for t in local_teams_from_pt + local_teams_from_sp:
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

        for comp in competitors:
            fcms_team_id = comp.get("teamId")
            # title/internationalTitle are at competitor level, not nested in "team"
            title = comp.get("title", "")
            int_title = comp.get("internationalTitle", "")

            local = match_team(title, int_title, local_teams)
            if local:
                matched += 1
                status = "✓ MATCHED"
                if local.fcms_team_id == fcms_team_id:
                    status = "✓ ALREADY SET"
                elif local.fcms_team_id and local.fcms_team_id != fcms_team_id:
                    status = f"⚠️ CONFLICT (current={local.fcms_team_id})"
                else:
                    sql = f"UPDATE teams SET fcms_team_id = {fcms_team_id} WHERE id = {local.id};  -- {local.name}"
                    sql_lines.append(sql)

                    if not dry_run:
                        await db.execute(
                            update(Team).where(Team.id == local.id).values(fcms_team_id=fcms_team_id)
                        )

                print(f"  FCMS {fcms_team_id}: {title} ({int_title}) → {local.name} (id={local.id}) {status}")
            else:
                unmatched.append((fcms_team_id, title, int_title))
                print(f"  FCMS {fcms_team_id}: {title} ({int_title}) → ❌ NOT FOUND")

        if not dry_run:
            await db.commit()
            print(f"\n✅ Applied {len(sql_lines)} mappings to database")
        else:
            print(f"\n[DRY RUN] Would apply {len(sql_lines)} mappings")

        print(f"\nMatched: {matched}/{len(competitors)}")
        if unmatched:
            print(f"\nUnmatched ({len(unmatched)}):")
            for tid, title, int_title in unmatched:
                print(f"  FCMS {tid}: {title} / {int_title}")

        if sql_lines:
            print(f"\n{'='*60}")
            print("SQL to add to migrate_fcms_columns.sql:")
            print(f"{'='*60}")
            for sql in sql_lines:
                print(sql)


if __name__ == "__main__":
    asyncio.run(main())
