"""
Read-only investigation script for season 80:
1. Find duplicate players (same person, different player_ids)
2. Find unresolved players (events with no player_teams record)
3. Try to match unresolved players to existing players table entries
"""

import asyncio
from collections import defaultdict
from app.database import AsyncSessionLocal
from sqlalchemy import text


# Team name -> team_id mapping for season 80
TEAM_MAP = {
    'SD Family Ж': 317, 'Алтай Ж': 629, 'Арыс': 333, 'Астана Ж': 626,
    'Атырау Ж': 634, 'Елімай Ж': 623, 'Жайық': 633, 'Жас Қыран': 632,
    'Жеңіс Ж': 619, 'Жетісу Ж': 627, 'Jeyran': 625, 'Каспий Ж': 624,
    'Қайсар Ж': 332, 'Қызылжар Ж': 316, 'Мақтаарал': 630, 'Оқжетпес Ж': 315,
    'Ордабасы Ж': 620, 'Талас': 622, 'Тобыл Ж': 621, 'Тұран Ж': 631,
    'Ұлытау Ж': 635, 'Шахтёр Ж': 628,
}

# Kazakh -> Russian letter transliteration (common substitutions)
KZ_TO_RU = {
    'ә': 'а', 'Ә': 'А',
    'ғ': 'г', 'Ғ': 'Г',
    'қ': 'к', 'Қ': 'К',
    'ң': 'н', 'Ң': 'Н',
    'ө': 'о', 'Ө': 'О',
    'ұ': 'у', 'Ұ': 'У',
    'ү': 'у', 'Ү': 'У',
    'і': 'и', 'І': 'И',
    'һ': 'х', 'Һ': 'Х',
}


def transliterate_kz_to_ru(name: str) -> str:
    """Transliterate Kazakh-specific letters to Russian equivalents."""
    result = []
    for ch in name:
        result.append(KZ_TO_RU.get(ch, ch))
    return ''.join(result)


async def main():
    async with AsyncSessionLocal() as session:
        # =====================================================
        # PART 1: Get all events for season 80
        # =====================================================
        print("=" * 80)
        print("PART 1: DUPLICATE PLAYER DETECTION FOR SEASON 80")
        print("=" * 80)

        # Get all goal/card events for season 80 with player info
        events_q = text("""
            SELECT ge.player_name, ge.player_id, ge.player_number, ge.team_name,
                   ge.event_type, ge.game_id
            FROM game_events ge
            JOIN games g ON ge.game_id = g.id
            WHERE g.season_id = 80
              AND ge.event_type IN ('goal', 'yellow_card', 'red_card')
              AND ge.player_name IS NOT NULL
            ORDER BY ge.player_name
        """)
        result = await session.execute(events_q)
        events = result.fetchall()
        print(f"\nTotal events (goal/yellow/red) in season 80: {len(events)}")

        # =====================================================
        # 1a: Find names that map to multiple player_ids
        # =====================================================
        name_to_pids = defaultdict(set)
        name_to_events = defaultdict(list)
        for row in events:
            pname, pid, pnum, tname, etype, gid = row
            if pid is not None:
                name_to_pids[pname].add(pid)
            name_to_events[pname].append({
                'pid': pid, 'pnum': pnum, 'team': tname, 'type': etype, 'gid': gid
            })

        print("\n--- Names resolving to MULTIPLE player_ids ---")
        multi_pid_names = {n: pids for n, pids in name_to_pids.items() if len(pids) > 1}
        if multi_pid_names:
            for name, pids in sorted(multi_pid_names.items()):
                print(f"  '{name}' -> player_ids: {sorted(pids)}")
        else:
            print("  (none found)")

        # =====================================================
        # 1b: Find different names with same number + team
        #     (likely same person with spelling variants)
        # =====================================================
        print("\n--- Different names with SAME player_number + team (likely duplicates) ---")

        # Build (team, number) -> set of (name, pid) 
        team_num_to_names = defaultdict(lambda: {'names': set(), 'pids': set()})
        for row in events:
            pname, pid, pnum, tname, etype, gid = row
            if pnum is not None and tname is not None:
                team_num_to_names[(tname, pnum)]['names'].add(pname)
                if pid is not None:
                    team_num_to_names[(tname, pnum)]['pids'].add(pid)

        duplicate_groups = []
        for (team, num), info in sorted(team_num_to_names.items()):
            names = info['names']
            pids = info.get('pids', set())
            if len(names) > 1:
                # Count events per name
                name_counts = {}
                for n in names:
                    goals = sum(1 for e in name_to_events[n] if e['type'] == 'goal' and e['team'] == team)
                    total = sum(1 for e in name_to_events[n] if e['team'] == team)
                    name_counts[n] = {'goals': goals, 'total_events': total}

                duplicate_groups.append({
                    'team': team,
                    'number': num,
                    'names': sorted(names),
                    'player_ids': sorted(pids),
                    'event_counts': name_counts,
                })
                print(f"\n  Team: {team}, Number: {num}")
                print(f"    Player IDs: {sorted(pids)}")
                for n in sorted(names):
                    c = name_counts[n]
                    pid_for_name = name_to_pids.get(n, set())
                    print(f"    '{n}' (pid={sorted(pid_for_name)}) -> goals={c['goals']}, total_events={c['total_events']}")

        # =====================================================
        # 1c: Also look for different names with the same player_id
        #     on the same team (spelling variants already resolved)
        # =====================================================
        print("\n\n--- Different names sharing the SAME player_id (already linked, but spelling variants) ---")
        pid_to_names = defaultdict(set)
        for row in events:
            pname, pid, pnum, tname, etype, gid = row
            if pid is not None:
                pid_to_names[pid].add(pname)

        for pid, names in sorted(pid_to_names.items()):
            if len(names) > 1:
                print(f"  player_id={pid}: {sorted(names)}")

        # =====================================================
        # PART 2: UNRESOLVED PLAYERS (no player_teams record)
        # =====================================================
        print("\n\n" + "=" * 80)
        print("PART 2: UNRESOLVED PLAYERS (events exist, no player_teams for season 80)")
        print("=" * 80)

        # Get all distinct player_name + team combos from events that have NO player_id
        unresolved_q = text("""
            SELECT ge.player_name, ge.player_number, ge.team_name,
                   COUNT(*) as event_count,
                   SUM(CASE WHEN ge.event_type = 'goal' THEN 1 ELSE 0 END) as goals,
                   SUM(CASE WHEN ge.event_type = 'yellow_card' THEN 1 ELSE 0 END) as yellows,
                   SUM(CASE WHEN ge.event_type = 'red_card' THEN 1 ELSE 0 END) as reds
            FROM game_events ge
            JOIN games g ON ge.game_id = g.id
            WHERE g.season_id = 80
              AND ge.event_type IN ('goal', 'yellow_card', 'red_card')
              AND ge.player_name IS NOT NULL
              AND ge.player_id IS NULL
            GROUP BY ge.player_name, ge.player_number, ge.team_name
            ORDER BY ge.team_name, ge.player_name
        """)
        result = await session.execute(unresolved_q)
        unresolved = result.fetchall()
        print(f"\nUnresolved player-events (no player_id): {len(unresolved)}")

        # Also get players who HAVE player_id but NO player_teams record for season 80
        no_pt_q = text("""
            SELECT ge.player_name, ge.player_id, ge.player_number, ge.team_name,
                   COUNT(*) as event_count,
                   SUM(CASE WHEN ge.event_type = 'goal' THEN 1 ELSE 0 END) as goals
            FROM game_events ge
            JOIN games g ON ge.game_id = g.id
            WHERE g.season_id = 80
              AND ge.event_type IN ('goal', 'yellow_card', 'red_card')
              AND ge.player_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM player_teams pt
                  WHERE pt.player_id = ge.player_id AND pt.season_id = 80
              )
            GROUP BY ge.player_name, ge.player_id, ge.player_number, ge.team_name
            ORDER BY ge.team_name, ge.player_name
        """)
        result = await session.execute(no_pt_q)
        no_pt_records = result.fetchall()
        print(f"Players WITH player_id but NO player_teams for season 80: {len(no_pt_records)}")

        if no_pt_records:
            print("\n  These players have player_id but missing player_teams records:")
            for row in no_pt_records:
                pname, pid, pnum, tname, ecount, goals = row
                print(f"    '{pname}' pid={pid} #{pnum} team='{tname}' events={ecount} goals={goals}")

        # =====================================================
        # PART 3: Try to match unresolved players to players table
        # =====================================================
        print("\n\n" + "=" * 80)
        print("PART 3: MATCHING UNRESOLVED PLAYERS TO PLAYERS TABLE")
        print("=" * 80)

        found_candidates = []
        not_found = []

        for row in unresolved:
            pname, pnum, tname, ecount, goals, yellows, reds = row
            parts = pname.strip().split()
            if len(parts) < 2:
                first_part = parts[0] if parts else pname
                last_part = ''
            else:
                first_part = parts[0]
                last_part = parts[-1]

            # Transliterate Kazakh to Russian
            first_ru = transliterate_kz_to_ru(first_part)
            last_ru = transliterate_kz_to_ru(last_part)

            team_id = TEAM_MAP.get(tname)

            # Search by last name (both original and transliterated)
            candidates = []

            # Strategy 1: Search by last_name ILIKE (original)
            if last_part:
                q1 = text("""
                    SELECT p.id, p.first_name, p.last_name, p.first_name_kz, p.last_name_kz
                    FROM players p
                    WHERE (p.last_name ILIKE :last_pattern 
                           OR p.last_name_kz ILIKE :last_pattern
                           OR p.last_name ILIKE :last_ru_pattern
                           OR p.last_name_kz ILIKE :last_ru_pattern)
                    ORDER BY p.id
                """)
                result = await session.execute(q1, {
                    'last_pattern': f'%{last_part}%',
                    'last_ru_pattern': f'%{last_ru}%',
                })
                for r in result.fetchall():
                    candidates.append({
                        'pid': r[0], 'first_name': r[1], 'last_name': r[2],
                        'first_name_kz': r[3], 'last_name_kz': r[4],
                        'match_type': 'last_name'
                    })

            # Strategy 2: If we have a team_id, check player_teams for that team (any season)
            if team_id and not candidates:
                q2 = text("""
                    SELECT p.id, p.first_name, p.last_name, p.first_name_kz, p.last_name_kz, pt.season_id
                    FROM players p
                    JOIN player_teams pt ON pt.player_id = p.id
                    WHERE pt.team_id = :team_id
                      AND (p.last_name ILIKE :last_pattern
                           OR p.last_name_kz ILIKE :last_pattern
                           OR p.last_name ILIKE :last_ru_pattern
                           OR p.last_name_kz ILIKE :last_ru_pattern
                           OR p.first_name ILIKE :first_pattern
                           OR p.first_name_kz ILIKE :first_pattern)
                    ORDER BY p.id
                """)
                result = await session.execute(q2, {
                    'team_id': team_id,
                    'last_pattern': f'%{last_part}%',
                    'last_ru_pattern': f'%{last_ru}%',
                    'first_pattern': f'%{first_part}%',
                })
                for r in result.fetchall():
                    candidates.append({
                        'pid': r[0], 'first_name': r[1], 'last_name': r[2],
                        'first_name_kz': r[3], 'last_name_kz': r[4],
                        'match_type': f'team+name (season {r[5]})'
                    })

            # Strategy 3: If still nothing, try first name as last name and vice versa
            if not candidates and last_part:
                q3 = text("""
                    SELECT p.id, p.first_name, p.last_name, p.first_name_kz, p.last_name_kz
                    FROM players p
                    WHERE (p.first_name ILIKE :last_pattern 
                           OR p.first_name_kz ILIKE :last_pattern
                           OR p.first_name ILIKE :last_ru_pattern
                           OR p.first_name_kz ILIKE :last_ru_pattern)
                      AND (p.last_name ILIKE :first_pattern
                           OR p.last_name_kz ILIKE :first_pattern
                           OR p.last_name ILIKE :first_ru_pattern
                           OR p.last_name_kz ILIKE :first_ru_pattern)
                    ORDER BY p.id
                """)
                first_ru2 = transliterate_kz_to_ru(first_part)
                result = await session.execute(q3, {
                    'last_pattern': f'%{last_part}%',
                    'last_ru_pattern': f'%{last_ru}%',
                    'first_pattern': f'%{first_part}%',
                    'first_ru_pattern': f'%{first_ru2}%',
                })
                for r in result.fetchall():
                    candidates.append({
                        'pid': r[0], 'first_name': r[1], 'last_name': r[2],
                        'first_name_kz': r[3], 'last_name_kz': r[4],
                        'match_type': 'swapped_name'
                    })

            if candidates:
                # Deduplicate by pid
                seen = set()
                unique_candidates = []
                for c in candidates:
                    if c['pid'] not in seen:
                        seen.add(c['pid'])
                        unique_candidates.append(c)
                found_candidates.append({
                    'event_name': pname,
                    'event_number': pnum,
                    'event_team': tname,
                    'event_count': ecount,
                    'goals': goals,
                    'yellows': yellows,
                    'reds': reds,
                    'candidates': unique_candidates,
                })
            else:
                not_found.append({
                    'event_name': pname,
                    'event_number': pnum,
                    'event_team': tname,
                    'event_count': ecount,
                    'goals': goals,
                    'yellows': yellows,
                    'reds': reds,
                })

        # =====================================================
        # OUTPUT RESULTS
        # =====================================================
        print(f"\n\n--- MATCHABLE: {len(found_candidates)} unresolved players with candidates in players table ---")
        for item in found_candidates:
            print(f"\n  Event: '{item['event_name']}' #{item['event_number']} team='{item['event_team']}'")
            print(f"    Events: {item['event_count']} (goals={item['goals']}, yellows={item['yellows']}, reds={item['reds']})")
            team_id = TEAM_MAP.get(item['event_team'])
            for c in item['candidates']:
                print(f"    -> Candidate pid={c['pid']}: {c['first_name']} {c['last_name']} "
                      f"(kz: {c['first_name_kz']} {c['last_name_kz']}) [{c['match_type']}]")

        print(f"\n\n--- NOT FOUND: {len(not_found)} unresolved players with NO matches in players table ---")
        for item in not_found:
            print(f"  '{item['event_name']}' #{item['event_number']} team='{item['event_team']}' "
                  f"events={item['event_count']} (goals={item['goals']}, yellows={item['yellows']}, reds={item['reds']})")

        # =====================================================
        # PART 4: SUMMARY OF CONFIRMED DUPLICATES
        # =====================================================
        print("\n\n" + "=" * 80)
        print("PART 4: CONFIRMED DUPLICATE GROUPS (same number + team, different names/pids)")
        print("=" * 80)

        for i, group in enumerate(duplicate_groups, 1):
            pids = group['player_ids']
            if len(pids) > 1:
                # Fetch player details
                pid_list = ','.join(str(p) for p in pids)
                pq = text(f"""
                    SELECT p.id, p.first_name, p.last_name, p.first_name_kz, p.last_name_kz
                    FROM players p WHERE p.id IN ({pid_list})
                """)
                result = await session.execute(pq)
                player_details = result.fetchall()

                print(f"\n  Group {i}: Team '{group['team']}', Number #{group['number']}")
                print(f"    Names in events: {group['names']}")
                print(f"    Player IDs: {pids}")
                for pd in player_details:
                    print(f"      pid={pd[0]}: {pd[1]} {pd[2]} (kz: {pd[3]} {pd[4]})")

                # Total goals across all names
                total_goals = sum(c.get('goals', 0) for c in group['event_counts'].values())
                total_events = sum(c.get('total_events', 0) for c in group['event_counts'].values())
                print(f"    Total: {total_goals} goals, {total_events} events")

        # Also show groups where there's only 1 pid but multiple names (cosmetic issue)
        print("\n\n--- Cosmetic duplicates (same pid, multiple name spellings in events, same number+team) ---")
        for group in duplicate_groups:
            if len(group['player_ids']) <= 1:
                print(f"  Team '{group['team']}', #{group['number']}: {group['names']} -> pids={group['player_ids']}")

        # =====================================================
        # PART 5: Additional cross-check: player_ids appearing
        # on multiple teams in season 80
        # =====================================================
        print("\n\n" + "=" * 80)
        print("PART 5: PLAYERS ON MULTIPLE TEAMS IN SEASON 80 (from events)")
        print("=" * 80)

        pid_teams_q = text("""
            SELECT DISTINCT ge.player_id, ge.player_name, ge.team_name
            FROM game_events ge
            JOIN games g ON ge.game_id = g.id
            WHERE g.season_id = 80
              AND ge.player_id IS NOT NULL
              AND ge.event_type IN ('goal', 'yellow_card', 'red_card')
            ORDER BY ge.player_id
        """)
        result = await session.execute(pid_teams_q)
        pid_team_rows = result.fetchall()

        pid_to_teams = defaultdict(set)
        pid_to_name_samples = defaultdict(set)
        for r in pid_team_rows:
            pid_to_teams[r[0]].add(r[2])
            pid_to_name_samples[r[0]].add(r[1])

        multi_team = {pid: teams for pid, teams in pid_to_teams.items() if len(teams) > 1}
        if multi_team:
            for pid, teams in sorted(multi_team.items()):
                names = pid_to_name_samples[pid]
                print(f"  pid={pid}: teams={sorted(teams)}, names={sorted(names)}")
        else:
            print("  (none found)")

        # =====================================================
        # FINAL SUMMARY
        # =====================================================
        print("\n\n" + "=" * 80)
        print("FINAL SUMMARY")
        print("=" * 80)
        print(f"  Duplicate groups (same number+team, multiple pids): "
              f"{sum(1 for g in duplicate_groups if len(g['player_ids']) > 1)}")
        print(f"  Cosmetic duplicates (same pid, multiple spellings): "
              f"{sum(1 for g in duplicate_groups if len(g['player_ids']) <= 1)}")
        print(f"  Unresolved with NULL player_id: {len(unresolved)}")
        print(f"    - Matchable to players table: {len(found_candidates)}")
        print(f"    - Truly not found: {len(not_found)}")
        print(f"  Players with pid but no player_teams for season 80: {len(no_pt_records)}")
        print(f"  Players on multiple teams: {len(multi_team)}")


if __name__ == "__main__":
    asyncio.run(main())
