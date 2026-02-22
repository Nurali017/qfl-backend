"""
Fix duplicate players and resolve NULL player_id events for season 80.

Problems fixed:
1. Merge 19 duplicate player groups (same person, different player_ids due to
   Kazakh vs Russian name spellings)
2. Round 2: Fix top-scorer photos in season 80 (10 duplicates with goals on
   wrong pid while photos are on the canonical pid)
3. Resolve NULL player_id in game_events using player_number + team
4. For players without player_teams records, match by name and create records

Usage:
    python3 fix_player_duplicates.py --season-id 80 --dry-run
    python3 fix_player_duplicates.py --season-id 80 --apply
"""
import argparse
import asyncio
from collections import defaultdict
from sqlalchemy import text
from app.database import AsyncSessionLocal


# Canonical player_id ← duplicate player_id
# Canonical is the one with player_teams records / more history
DUPLICATE_MERGES = {
    # duplicate_pid: canonical_pid
    # --- Round 1 (previous merges) ---
    7: 517,      # Амангельдинов: pid=7 → pid=517 (has player_teams num=78)
    2477: 792,   # Валихан: pid=2477 → pid=792 (has player_teams num=82)
    2472: 2321,  # Масан: pid=2472 → pid=2321
    2474: 913,   # Кундаков: pid=2474 → pid=913 (has player_teams num=53)
    2428: 976,   # Камарадинов: pid=2428 → pid=976 (has player_teams across seasons)
    2425: 437,   # Кайратов: pid=2425 → pid=437 (cascaded: was →426, now 426→437)
    2433: 630,   # Сердалы: pid=2433 → pid=630 (has player_teams num=55)
    2352: 940,   # Халдар: pid=2352 → pid=940 (has player_teams across seasons)
    2323: 1725,  # Молдабаев: pid=2323 → pid=1725 (has player_teams num=18)
    # --- Round 2 (season 80 top-scorer photo fixes) ---
    2401: 1665,  # Нурадилов Нурали: 15 голов, фото у 1665
    2410: 392,   # Алихан Амир: 7 голов, фото у 392
    426: 437,    # Кайратов Мирас: 5 голов, фото у 437 (cascade from 2425→426)
    2354: 684,   # Кубашев Чингиз: 5 голов, фото у 684
    2389: 132,   # Кабиев Хакназар: 4 голов, фото у 132
    2385: 619,   # Бердеш Нурсейт: 4 голов, фото у 619
    2320: 1835,  # Талгат Айбар: 2 голов, фото у 1835
    2368: 1468,  # Молдагалиев Арафат: 2 голов, фото у 1468
    2355: 838,   # Бердаулетов Самат: 2 голов, фото у 838
    1927: 213,   # Саидалиев Юсуф: 1 гол, фото у 213
}

# Event team_name (Kazakh) → team_id
TEAM_MAP = {
    "SD Family Ж": 317, "Алтай Ж": 629, "Арыс": 333, "Астана Ж": 626,
    "Атырау Ж": 634, "Елімай Ж": 623, "Жайық": 633, "Жас Қыран": 632,
    "Жеңіс Ж": 619, "Жетісу Ж": 627, "Jeyran": 625, "Каспий Ж": 624,
    "Қайсар Ж": 332, "Қызылжар Ж": 316, "Мақтаарал": 630, "Оқжетпес Ж": 315,
    "Ордабасы Ж": 620, "Талас": 622, "Тобыл Ж": 621, "Тұран Ж": 631,
    "Ұлытау Ж": 635, "Шахтёр Ж": 628,
}

# Common Kazakh → Russian letter replacements for name matching
KZ_TO_RU = str.maketrans({
    "Қ": "К", "қ": "к",
    "Ғ": "Г", "ғ": "г",
    "Ң": "Н", "ң": "н",
    "Ү": "У", "ү": "у",
    "Ұ": "У", "ұ": "у",
    "Ә": "А", "ә": "а",
    "Ө": "О", "ө": "о",
    "І": "И", "і": "и",
    "Һ": "Х", "һ": "х",
})


def kz_to_ru(name: str) -> str:
    """Transliterate Kazakh-specific letters to Russian equivalents."""
    return name.translate(KZ_TO_RU)


async def main(season_id: int, apply: bool):
    async with AsyncSessionLocal() as session:
        # ═══════════════════════════════════════════════════════════════
        # PART 1: Merge duplicate player_ids in game_events
        # ═══════════════════════════════════════════════════════════════
        print("=" * 70)
        print("PART 1: MERGE DUPLICATE PLAYERS")
        print("=" * 70)

        # Get all game_ids for this season
        games_result = await session.execute(text(
            "SELECT id FROM games WHERE season_id = :sid"
        ), {"sid": season_id})
        season_game_ids = [r.id for r in games_result.fetchall()]

        merge_stats = {"events_player": 0, "events_assist": 0,
                       "events_player2": 0, "gps": 0, "pss_deleted": 0}

        for dup_pid, canon_pid in DUPLICATE_MERGES.items():
            # Count affected events
            r = await session.execute(text(
                "SELECT COUNT(*) FROM game_events "
                "WHERE player_id = :dup AND game_id = ANY(:gids)"
            ), {"dup": dup_pid, "gids": season_game_ids})
            evt_count = r.scalar()

            r = await session.execute(text(
                "SELECT COUNT(*) FROM game_events "
                "WHERE assist_player_id = :dup AND game_id = ANY(:gids)"
            ), {"dup": dup_pid, "gids": season_game_ids})
            assist_count = r.scalar()

            r = await session.execute(text(
                "SELECT COUNT(*) FROM game_events "
                "WHERE player2_id = :dup AND game_id = ANY(:gids)"
            ), {"dup": dup_pid, "gids": season_game_ids})
            p2_count = r.scalar()

            r = await session.execute(text(
                "SELECT COUNT(*) FROM game_player_stats "
                "WHERE player_id = :dup AND game_id = ANY(:gids)"
            ), {"dup": dup_pid, "gids": season_game_ids})
            gps_count = r.scalar()

            # Get names for display
            r = await session.execute(text(
                "SELECT id, first_name, last_name FROM players WHERE id IN (:p1, :p2)"
            ), {"p1": dup_pid, "p2": canon_pid})
            names = {row.id: f"{row.first_name} {row.last_name}" for row in r.fetchall()}

            total = evt_count + assist_count + p2_count + gps_count
            if total > 0:
                print(f"  {names.get(dup_pid, dup_pid)} (pid={dup_pid}) → "
                      f"{names.get(canon_pid, canon_pid)} (pid={canon_pid}): "
                      f"{evt_count} events, {assist_count} assists, "
                      f"{p2_count} player2, {gps_count} gps")

            merge_stats["events_player"] += evt_count
            merge_stats["events_assist"] += assist_count
            merge_stats["events_player2"] += p2_count
            merge_stats["gps"] += gps_count

        print(f"\n  Total: {merge_stats['events_player']} player events, "
              f"{merge_stats['events_assist']} assist events, "
              f"{merge_stats['events_player2']} player2 events, "
              f"{merge_stats['gps']} game_player_stats")

        # ═══════════════════════════════════════════════════════════════
        # PART 2: Resolve NULL player_id events
        # ═══════════════════════════════════════════════════════════════
        print()
        print("=" * 70)
        print("PART 2: RESOLVE NULL player_id EVENTS")
        print("=" * 70)

        # Build team_map dynamically (supplement with hardcoded)
        tm_result = await session.execute(text("""
            SELECT DISTINCT ge.team_name, ge.team_id
            FROM game_events ge JOIN games g ON g.id = ge.game_id
            WHERE g.season_id = :sid AND ge.team_name IS NOT NULL AND ge.team_id IS NOT NULL
        """), {"sid": season_id})
        team_map = {row.team_name: row.team_id for row in tm_result.fetchall()}
        for k, v in TEAM_MAP.items():
            team_map.setdefault(k, v)

        # Build (team_id, number) → player_id from player_teams (ALL seasons)
        pt_result = await session.execute(text("""
            SELECT pt.player_id, pt.team_id, pt.number
            FROM player_teams pt
            WHERE pt.number IS NOT NULL
        """))
        pt_lookup = {}
        for row in pt_result.fetchall():
            # Prefer season 80 records, but keep any
            key = (row.team_id, row.number)
            pt_lookup.setdefault(key, row.player_id)

        # Get ALL NULL player_id events
        null_events = await session.execute(text("""
            SELECT ge.id, ge.player_name, ge.player_number, ge.team_name,
                   ge.event_type
            FROM game_events ge
            JOIN games g ON g.id = ge.game_id
            WHERE g.season_id = :sid AND ge.player_id IS NULL
              AND ge.player_name IS NOT NULL
        """), {"sid": season_id})
        null_rows = null_events.fetchall()

        # Strategy A: player_number + team → player_teams
        resolved_a = {}  # event_id → player_id
        unresolved_events = []

        for row in null_rows:
            tid = team_map.get(row.team_name)
            if tid and row.player_number:
                pid = pt_lookup.get((tid, row.player_number))
                if pid:
                    # Apply dedup mapping
                    pid = DUPLICATE_MERGES.get(pid, pid)
                    resolved_a[row.id] = pid
                    continue
            unresolved_events.append(row)

        print(f"  NULL player_id events: {len(null_rows)}")
        print(f"  → Resolved via number+team: {len(resolved_a)}")
        print(f"  → Still unresolved: {len(unresolved_events)}")

        # Strategy B: Match player_name against players table
        # Build a reverse lookup: (team_id, transliterated_last_name) → player_id
        resolved_b = {}
        still_unresolved = []

        if unresolved_events:
            # Get all players on teams in this season
            team_players = await session.execute(text("""
                SELECT p.id, p.first_name, p.last_name, pt.team_id, pt.number
                FROM players p
                JOIN player_teams pt ON pt.player_id = p.id
                WHERE pt.team_id = ANY(:tids)
            """), {"tids": list(set(team_map.values()))})

            # Build lookup by team + last_name (Russian)
            team_name_lookup = defaultdict(list)  # (team_id, last_name_lower) → [(pid, first_name)]
            for row in team_players.fetchall():
                if row.last_name:
                    key = (row.team_id, row.last_name.lower())
                    team_name_lookup[key].append((row.id, row.first_name or ""))

            for row in unresolved_events:
                tid = team_map.get(row.team_name)
                if not tid or not row.player_name:
                    still_unresolved.append(row)
                    continue

                # Split "Ерболат Құралбаев" → first="Ерболат", last="Құралбаев"
                parts = row.player_name.strip().split()
                if len(parts) < 2:
                    still_unresolved.append(row)
                    continue

                first_kz = parts[0]
                last_kz = parts[-1]
                first_ru = kz_to_ru(first_kz).lower()
                last_ru = kz_to_ru(last_kz).lower()

                candidates = team_name_lookup.get((tid, last_ru), [])
                if len(candidates) == 1:
                    pid = candidates[0][0]
                    pid = DUPLICATE_MERGES.get(pid, pid)
                    resolved_b[row.id] = pid
                elif len(candidates) > 1:
                    # Try matching first name too
                    matched = [
                        (pid, fn) for pid, fn in candidates
                        if fn and fn.lower() == first_ru
                    ]
                    if len(matched) == 1:
                        pid = matched[0][0]
                        pid = DUPLICATE_MERGES.get(pid, pid)
                        resolved_b[row.id] = pid
                    else:
                        still_unresolved.append(row)
                else:
                    still_unresolved.append(row)

            print(f"  → Resolved via name matching: {len(resolved_b)}")
            print(f"  → Truly unresolved: {len(still_unresolved)}")

        # Show unresolved with goals
        unresolved_with_goals = [r for r in still_unresolved if r.event_type == "goal"]
        if unresolved_with_goals:
            # Group by player_name
            by_name = defaultdict(lambda: {"goals": 0, "team": "", "num": 0})
            for r in unresolved_with_goals:
                by_name[r.player_name]["goals"] += 1
                by_name[r.player_name]["team"] = r.team_name
                by_name[r.player_name]["num"] = r.player_number
            print(f"\n  Unresolved goal scorers ({len(by_name)}):")
            for pname, d in sorted(by_name.items(), key=lambda x: -x[1]["goals"]):
                if d["goals"] >= 1:
                    print(f"    {pname} #{d['num']} ({d['team']}): {d['goals']}g")

        # ═══════════════════════════════════════════════════════════════
        # PART 3: Create missing player_teams records
        # ═══════════════════════════════════════════════════════════════
        print()
        print("=" * 70)
        print("PART 3: CREATE MISSING player_teams RECORDS")
        print("=" * 70)

        # Find players who have events resolved but no player_teams for this season
        all_resolved_pids = set(resolved_a.values()) | set(resolved_b.values())
        if all_resolved_pids:
            existing_pt = await session.execute(text(
                "SELECT DISTINCT player_id FROM player_teams WHERE season_id = :sid"
            ), {"sid": season_id})
            existing_pt_pids = {r.player_id for r in existing_pt.fetchall()}

            missing_pt_pids = all_resolved_pids - existing_pt_pids
            print(f"  Players resolved but missing player_teams: {len(missing_pt_pids)}")

            # For each, determine team_id and number from events
            pt_to_create = []
            for evt_id, pid in {**resolved_a, **resolved_b}.items():
                if pid in missing_pt_pids:
                    # Find the event row
                    for row in null_rows:
                        if row.id == evt_id:
                            tid = team_map.get(row.team_name)
                            if tid:
                                pt_to_create.append((pid, tid, row.player_number))
                                missing_pt_pids.discard(pid)
                            break

            # Deduplicate
            seen = set()
            unique_pt = []
            for pid, tid, num in pt_to_create:
                if (pid, tid) not in seen:
                    seen.add((pid, tid))
                    unique_pt.append((pid, tid, num))

            if unique_pt:
                print(f"  Will create {len(unique_pt)} player_teams records")
                for pid, tid, num in unique_pt[:10]:
                    r = await session.execute(text(
                        "SELECT first_name, last_name FROM players WHERE id = :pid"
                    ), {"pid": pid})
                    p = r.fetchone()
                    name = f"{p.first_name} {p.last_name}" if p else f"pid={pid}"
                    r2 = await session.execute(text(
                        "SELECT name FROM teams WHERE id = :tid"
                    ), {"tid": tid})
                    tname = r2.scalar() or f"tid={tid}"
                    print(f"    {name} → {tname} #{num}")
                if len(unique_pt) > 10:
                    print(f"    ... and {len(unique_pt) - 10} more")

        # ═══════════════════════════════════════════════════════════════
        # SUMMARY
        # ═══════════════════════════════════════════════════════════════
        print()
        print("=" * 70)
        print("SUMMARY")
        print("=" * 70)
        print(f"  Duplicate merges: {len(DUPLICATE_MERGES)} groups")
        print(f"  Events to update (player_id):   {merge_stats['events_player']}")
        print(f"  Events to update (assist):      {merge_stats['events_assist']}")
        print(f"  Events to update (player2):     {merge_stats['events_player2']}")
        print(f"  Game player stats to update:    {merge_stats['gps']}")
        print(f"  NULL events resolved (num+team): {len(resolved_a)}")
        print(f"  NULL events resolved (name):     {len(resolved_b)}")
        total_resolved = len(resolved_a) + len(resolved_b)
        print(f"  Total NULL events resolved:      {total_resolved}/{len(null_rows)}")
        print(f"  Still unresolved:                {len(still_unresolved)}")
        print()

        if not apply:
            print("*** DRY RUN — no changes made. Use --apply to execute. ***")
            return

        # ═══════════════════════════════════════════════════════════════
        # APPLY CHANGES
        # ═══════════════════════════════════════════════════════════════
        print("Applying changes...")

        # 1. Merge duplicates in game_events
        for dup_pid, canon_pid in DUPLICATE_MERGES.items():
            await session.execute(text(
                "UPDATE game_events SET player_id = :canon "
                "WHERE player_id = :dup AND game_id = ANY(:gids)"
            ), {"canon": canon_pid, "dup": dup_pid, "gids": season_game_ids})

            await session.execute(text(
                "UPDATE game_events SET assist_player_id = :canon "
                "WHERE assist_player_id = :dup AND game_id = ANY(:gids)"
            ), {"canon": canon_pid, "dup": dup_pid, "gids": season_game_ids})

            await session.execute(text(
                "UPDATE game_events SET player2_id = :canon "
                "WHERE player2_id = :dup AND game_id = ANY(:gids)"
            ), {"canon": canon_pid, "dup": dup_pid, "gids": season_game_ids})

        # 2. Merge duplicates in game_player_stats
        for dup_pid, canon_pid in DUPLICATE_MERGES.items():
            # Check for conflicts (both exist for same game)
            conflicts = await session.execute(text(
                "SELECT gps1.game_id FROM game_player_stats gps1 "
                "JOIN game_player_stats gps2 ON gps1.game_id = gps2.game_id "
                "WHERE gps1.player_id = :dup AND gps2.player_id = :canon "
                "AND gps1.game_id = ANY(:gids)"
            ), {"dup": dup_pid, "canon": canon_pid, "gids": season_game_ids})
            conflict_games = [r.game_id for r in conflicts.fetchall()]

            if conflict_games:
                # Delete duplicate's records for conflicting games
                await session.execute(text(
                    "DELETE FROM game_player_stats "
                    "WHERE player_id = :dup AND game_id = ANY(:gids)"
                ), {"dup": dup_pid, "gids": conflict_games})

            # Update remaining
            await session.execute(text(
                "UPDATE game_player_stats SET player_id = :canon "
                "WHERE player_id = :dup AND game_id = ANY(:gids)"
            ), {"canon": canon_pid, "dup": dup_pid, "gids": season_game_ids})

        # 3. Delete duplicate player_season_stats (will be recalculated)
        dup_pids = list(DUPLICATE_MERGES.keys())
        await session.execute(text(
            "DELETE FROM player_season_stats "
            "WHERE player_id = ANY(:pids) AND season_id = :sid"
        ), {"pids": dup_pids, "sid": season_id})

        # 4. Resolve NULL player_id events
        for evt_id, pid in {**resolved_a, **resolved_b}.items():
            await session.execute(text(
                "UPDATE game_events SET player_id = :pid WHERE id = :eid"
            ), {"pid": pid, "eid": evt_id})

        # 5. Create missing player_teams records
        if unique_pt:
            for pid, tid, num in unique_pt:
                # Check not already created
                existing = await session.execute(text(
                    "SELECT id FROM player_teams "
                    "WHERE player_id = :pid AND team_id = :tid AND season_id = :sid"
                ), {"pid": pid, "tid": tid, "sid": season_id})
                if not existing.fetchone():
                    await session.execute(text(
                        "INSERT INTO player_teams (player_id, team_id, season_id, number) "
                        "VALUES (:pid, :tid, :sid, :num)"
                    ), {"pid": pid, "tid": tid, "sid": season_id, "num": num})

        await session.commit()
        print("Done. Now run: python3 recalculate_player_stats.py --season-id 80 --apply")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fix duplicate players and resolve NULL player_ids"
    )
    parser.add_argument(
        "--season-id", type=int, required=True, help="Season ID"
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--dry-run", action="store_true", default=True, help="Preview (default)"
    )
    group.add_argument("--apply", action="store_true", help="Apply changes")
    args = parser.parse_args()
    asyncio.run(main(season_id=args.season_id, apply=args.apply))
