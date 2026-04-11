"""FCMS roster sync — all competitions, auto-update safe fields, report to Telegram."""

import asyncio
import sys
import os
from datetime import date as date_type

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import select, or_, and_
from app.database import AsyncSessionLocal
from app.models.team import Team
from app.models.player import Player
from app.models.player_team import PlayerTeam
from app.services.fcms_client import FcmsClient
from app.services.telegram import send_telegram_message

# FCMS competition_id -> local season_id
COMPETITIONS = {
    3517: 200,   # Премьер-Лига 2026
    3585: 204,   # Первая Лига 2026
}


def _name_key(ln, fn):
    if ln and fn:
        return (ln.strip().lower(), fn.strip().lower())
    return None


def _find_in_roster(fn_ru, ln_ru, fn_en, ln_en, person_id, num, local_by_fcms, local_by_name, local_by_num):
    """Find player in current team roster."""
    if person_id and person_id in local_by_fcms:
        return local_by_fcms[person_id], "fcms_id"
    for ln, fn in [(ln_ru, fn_ru), (ln_en, fn_en)]:
        key = _name_key(ln, fn)
        if key and key in local_by_name:
            return local_by_name[key], "name"
    for ln, fn in [(ln_ru, fn_ru), (ln_en, fn_en)]:
        key = _name_key(fn, ln)
        if key and key in local_by_name:
            return local_by_name[key], "name_rev"
    if num and num in local_by_num:
        return local_by_num[num], "number"
    return None, None


async def _find_globally(db, fn_ru, ln_ru, fn_en, ln_en, person_id, dob, fcms_club_id):
    """Find player across entire DB. Returns (Player, method) or (None, None)."""
    # 1. By fcms_person_id
    if person_id:
        r = await db.execute(select(Player).where(Player.fcms_person_id == person_id))
        p = r.scalars().first()
        if p:
            return p, "global_fcms_id"

    # 2. By exact name (all lang variants, normal + reversed)
    for ln, fn in [(ln_ru, fn_ru), (ln_en, fn_en)]:
        if ln and fn:
            r = await db.execute(select(Player).where(
                or_(
                    and_(Player.last_name == ln, Player.first_name == fn),
                    and_(Player.last_name_en == ln, Player.first_name_en == fn),
                    and_(Player.last_name_kz == ln, Player.first_name_kz == fn),
                )
            ))
            p = r.scalars().first()
            if p:
                return p, "global_name"
    for ln, fn in [(ln_ru, fn_ru), (ln_en, fn_en)]:
        if ln and fn:
            r = await db.execute(select(Player).where(
                or_(
                    and_(Player.last_name == fn, Player.first_name == ln),
                    and_(Player.last_name_en == fn, Player.first_name_en == ln),
                )
            ))
            p = r.scalars().first()
            if p:
                return p, "global_name_rev"

    # 3. By date of birth + partial name match (last resort)
    if dob and ln_ru:
        dob_parsed = date_type.fromisoformat(dob)
        r = await db.execute(select(Player).where(
            Player.birthday == dob_parsed,
            or_(
                Player.last_name.ilike(f"%{ln_ru}%"),
                Player.last_name_en.ilike(f"%{ln_en}%") if ln_en else False,
            )
        ))
        p = r.scalars().first()
        if p:
            return p, "global_dob+name"

    return None, None


async def sync_team_roster(db, client, team, fcms_players, season_id):
    """Sync one team. Returns changes dict."""
    local_result = await db.execute(
        select(PlayerTeam, Player)
        .join(Player, PlayerTeam.player_id == Player.id)
        .where(
            PlayerTeam.team_id == team.id,
            PlayerTeam.season_id == season_id,
            PlayerTeam.is_active == True,
            PlayerTeam.is_hidden == False,
        )
    )
    local_roster = local_result.all()

    local_by_name = {}
    local_by_fcms = {}
    local_by_num = {}
    for pt, p in local_roster:
        for ln, fn in [
            (p.last_name, p.first_name),
            (p.last_name_en, p.first_name_en),
            (p.last_name_kz, p.first_name_kz),
        ]:
            key = _name_key(ln, fn)
            if key:
                local_by_name[key] = (pt, p)
        if p.fcms_person_id:
            local_by_fcms[p.fcms_person_id] = (pt, p)
        if pt.number:
            local_by_num[pt.number] = (pt, p)

    active_fcms = [fp for fp in fcms_players if fp.get("jerseyNumber")]
    changes = {
        "team_name": team.name,
        "fcms_total": len(fcms_players),
        "fcms_active": len(active_fcms),
        "local_count": len(local_roster),
        "auto_updates": [],
        "new_players": [],
        "deregistered": [],
        "matched": 0,
    }

    matched_fcms_ids = set()

    for fp in fcms_players:
        p = fp.get("player", {})
        fn_ru = p.get("localFirstName") or ""
        ln_ru = p.get("localFamilyName") or ""
        fn_en = p.get("firstName") or ""
        ln_en = p.get("familyName") or ""
        num_str = fp.get("jerseyNumber", "")
        num = int(num_str) if num_str else None
        dob = p.get("dateOfBirth", "")
        person_id = p.get("personId")
        fcms_club_id = p.get("clubId")
        fcms_name = f"{fn_ru} {ln_ru}".strip() or f"{fn_en} {ln_en}".strip()

        # Без номера = отзаявлен (аренда)
        if num is None:
            changes["deregistered"].append({"name": fcms_name, "person_id": person_id})
            continue

        # Search in current team roster
        match, method = _find_in_roster(
            fn_ru, ln_ru, fn_en, ln_en, person_id, num,
            local_by_fcms, local_by_name, local_by_num,
        )

        if match:
            pt, lp = match
        else:
            # Global search across entire DB
            lp, method = await _find_globally(db, fn_ru, ln_ru, fn_en, ln_en, person_id, dob, fcms_club_id)
            if lp:
                # Found globally — check if player_team exists for this season
                pt_result = await db.execute(
                    select(PlayerTeam).where(
                        PlayerTeam.player_id == lp.id,
                        PlayerTeam.team_id == team.id,
                        PlayerTeam.season_id == season_id,
                    )
                )
                pt = pt_result.scalars().first()
                if not pt:
                    # Create player_team link
                    pt = PlayerTeam(
                        player_id=lp.id,
                        team_id=team.id,
                        season_id=season_id,
                        number=num,
                        is_active=True,
                    )
                    db.add(pt)
                    await db.flush()
                    changes["auto_updates"].append({
                        "name": fcms_name,
                        "num": num_str,
                        "method": method,
                        "details": [f"привязан к {team.name} (id={lp.id}, {method})"],
                    })
                match = (pt, lp)

        if match or lp:
            if not match:
                match = (pt, lp)
            pt, lp = match
            changes["matched"] += 1
            matched_fcms_ids.add(lp.id)
            player_updates = []

            # --- AUTO: fcms_person_id ---
            if person_id and not lp.fcms_person_id:
                lp.fcms_person_id = person_id
                player_updates.append(f"fcms_id={person_id}")

            # --- AUTO: first_name / last_name (ru) ---
            if fn_ru and lp.first_name != fn_ru:
                player_updates.append(f"имя: {lp.first_name} → {fn_ru}")
                lp.first_name = fn_ru
            if ln_ru and lp.last_name != ln_ru:
                player_updates.append(f"фамилия: {lp.last_name} → {ln_ru}")
                lp.last_name = ln_ru

            # --- AUTO: en names ---
            if fn_en and lp.first_name_en != fn_en:
                player_updates.append(f"имя_en: {lp.first_name_en} → {fn_en}")
                lp.first_name_en = fn_en
            if ln_en and lp.last_name_en != ln_en:
                player_updates.append(f"фам_en: {lp.last_name_en} → {ln_en}")
                lp.last_name_en = ln_en

            # --- AUTO: birthday ---
            if dob:
                dob_parsed = date_type.fromisoformat(dob)
                if lp.birthday != dob_parsed:
                    player_updates.append(f"дата рожд: {lp.birthday} → {dob}")
                    lp.birthday = dob_parsed

            # --- AUTO: number ---
            if num is not None and pt.number != num:
                player_updates.append(f"номер: {pt.number} → {num}")
                pt.number = num

            if player_updates:
                # Don't duplicate if already added from global search
                existing = [u for u in changes["auto_updates"] if u["name"] == fcms_name]
                if existing:
                    existing[0]["details"].extend(player_updates)
                else:
                    changes["auto_updates"].append({
                        "name": fcms_name,
                        "num": num_str,
                        "method": method,
                        "details": player_updates,
                    })
        else:
            # Not found anywhere — report with FCMS context
            club_info = p.get("club", {})
            club_name = club_info.get("title") or club_info.get("internationalTitle") or ""
            changes["new_players"].append({
                "name": fcms_name,
                "num": num_str,
                "dob": dob,
                "person_id": person_id,
                "club": club_name,
            })

    # Players in our roster but NOT in FCMS active list
    missing = []
    for pt, p in local_roster:
        if p.id not in matched_fcms_ids:
            missing.append({
                "name": f"{p.first_name} {p.last_name}",
                "num": pt.number,
                "id": p.id,
            })
    changes["missing"] = missing

    return changes


def format_telegram_report(all_changes, comp_name):
    """Format all changes into Telegram message (HTML)."""
    has_any = False
    lines = [f"<b>📋 FCMS Roster Sync — {comp_name}</b>\n"]

    for ch in all_changes:
        team_lines = []

        if ch["auto_updates"]:
            has_any = True
            for p in ch["auto_updates"]:
                details = ", ".join(p["details"])
                team_lines.append(f"  ✏️ #{p['num']} {p['name']}: {details}")

        if ch["new_players"]:
            has_any = True
            for p in ch["new_players"]:
                club = f", клуб: {p['club']}" if p.get("club") else ""
                team_lines.append(f"  🆕 #{p['num']} {p['name']} ({p['dob']}{club}) — НЕ НАЙДЕН")

        if ch["missing"]:
            has_any = True
            for p in ch["missing"]:
                team_lines.append(f"  ❌ #{p['num']} {p['name']} (id={p['id']}) — нет в FCMS")

        if ch.get("deregistered"):
            for p in ch["deregistered"]:
                team_lines.append(f"  ⏸ {p['name']} — отзаявлен")

        if team_lines:
            lines.append(f"<b>{ch['team_name']}</b> ({ch['matched']}/{ch['fcms_active']} заявленных)")
            lines.extend(team_lines)
            lines.append("")

    if not has_any:
        lines.append("Изменений нет — все заявки актуальны ✅")

    return "\n".join(lines)


async def main():
    client = FcmsClient()

    comp_names = {3517: "Премьер-Лига 2026", 3585: "Первая Лига 2026"}

    for comp_id, season_id in COMPETITIONS.items():
        comp_name = comp_names[comp_id]
        all_changes = []

        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(Team).where(
                    Team.fcms_team_id.in_(
                        [c["teamId"] for c in await client.get_competition_competitors(comp_id)]
                    )
                ).order_by(Team.name)
            )
            teams = result.scalars().all()

            if not teams:
                print(f"\n{comp_name}: нет замапленных команд, skip")
                continue

            print(f"\n{'='*60}")
            print(f"  {comp_name} ({len(teams)} команд)")
            print(f"{'='*60}")

            for team in teams:
                print(f"  {team.name} (fcms={team.fcms_team_id})...", end=" ", flush=True)

                fcms_players = await client.get_competitor_players(comp_id, team.fcms_team_id)
                ch = await sync_team_roster(db, client, team, fcms_players, season_id)
                all_changes.append(ch)

                auto = len(ch["auto_updates"])
                new = len(ch["new_players"])
                miss = len(ch["missing"])
                dereg = len(ch["deregistered"])
                print(f"{ch['matched']}/{ch['fcms_active']} active, {auto} upd, {new} new, {miss} miss, {dereg} dereg")

            await db.commit()

        report = format_telegram_report(all_changes, comp_name)
        print(f"\n{report}")
        await send_telegram_message(report)

    await client.close()
    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
