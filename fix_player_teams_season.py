"""
Fix player_teams records with wrong season_id.

Many player_teams records point to Premier League (season_id=61) even though
the team participates in a different division (Second League, First League,
Women's League). This script finds and fixes those mismatches using
season_participants as the source of truth.

Usage:
    python3 fix_player_teams_season.py --dry-run   # default, preview changes
    python3 fix_player_teams_season.py --apply      # apply changes
"""
import argparse
import asyncio
from sqlalchemy import text
from app.database import AsyncSessionLocal


# Cup season IDs to exclude when resolving ambiguous team→season mappings.
# Teams like Каспий and Хан-Тенгри appear in both a Cup and a League;
# for roster bindings we want the league season.
CUP_CHAMPIONSHIP_KEYWORD = "кубок"


async def main(apply: bool):
    async with AsyncSessionLocal() as session:
        # Step 1: Find all wrong records — player_teams where team is NOT
        # in season_participants for that season_id, but IS in season_participants
        # for some other 2025 season.
        wrong_records = await session.execute(text("""
            SELECT
                pt.id AS pt_id,
                pt.player_id,
                pt.team_id,
                pt.season_id AS wrong_season_id,
                pt.number,
                t.name AS team_name,
                s_wrong.name AS wrong_season_name,
                c_wrong.name AS wrong_championship
            FROM player_teams pt
            JOIN teams t ON t.id = pt.team_id
            JOIN seasons s_wrong ON s_wrong.id = pt.season_id
            JOIN championships c_wrong ON c_wrong.id = s_wrong.championship_id
            LEFT JOIN season_participants sp_check
                ON sp_check.season_id = pt.season_id AND sp_check.team_id = pt.team_id
            WHERE s_wrong.name = '2025'
              AND sp_check.id IS NULL
            ORDER BY pt.team_id, pt.player_id
        """))
        wrong_rows = wrong_records.fetchall()

        if not wrong_rows:
            print("No wrong records found. Nothing to do.")
            return

        print(f"Found {len(wrong_rows)} wrong player_teams records.\n")

        # Step 2: For each wrong record, find the correct season_id via
        # season_participants (excluding cups for ambiguous teams).
        correct_seasons = await session.execute(text("""
            SELECT
                sp.team_id,
                sp.season_id AS correct_season_id,
                s.name AS season_name,
                c.name AS championship_name
            FROM season_participants sp
            JOIN seasons s ON s.id = sp.season_id
            JOIN championships c ON c.id = s.championship_id
            WHERE s.name = '2025'
            ORDER BY sp.team_id
        """))
        correct_rows = correct_seasons.fetchall()

        # Build team_id → list of (season_id, championship_name)
        team_correct_seasons: dict[int, list[tuple[int, str]]] = {}
        for row in correct_rows:
            team_correct_seasons.setdefault(row.team_id, []).append(
                (row.correct_season_id, row.championship_name)
            )

        # Resolve single correct season per team (prefer non-cup)
        team_correct_season: dict[int, int | None] = {}
        for team_id, options in team_correct_seasons.items():
            non_cup = [
                (sid, cname) for sid, cname in options
                if CUP_CHAMPIONSHIP_KEYWORD not in cname.lower()
            ]
            if len(non_cup) == 1:
                team_correct_season[team_id] = non_cup[0][0]
            elif len(options) == 1:
                team_correct_season[team_id] = options[0][0]
            else:
                # Ambiguous — multiple non-cup seasons; skip
                team_correct_season[team_id] = None

        # Step 3: Classify each wrong record
        updates = []      # (pt_id, correct_season_id) — UPDATE
        deletes = []      # (pt_id, correct_existing_pt_id, transfer_number) — DELETE wrong, maybe transfer number
        skipped = []      # (pt_id, reason)

        for row in wrong_rows:
            correct_sid = team_correct_season.get(row.team_id)
            if correct_sid is None:
                skipped.append((row.pt_id, f"team {row.team_name} (id={row.team_id}): no correct season found"))
                continue

            # Check if a correct record already exists for this player+team+correct_season
            existing = await session.execute(text("""
                SELECT id, number
                FROM player_teams
                WHERE player_id = :pid AND team_id = :tid AND season_id = :sid
            """), {"pid": row.player_id, "tid": row.team_id, "sid": correct_sid})
            existing_row = existing.fetchone()

            if existing_row:
                # Duplicate — correct record exists
                transfer_number = (
                    row.number is not None and existing_row.number is None
                )
                deletes.append((row.pt_id, existing_row.id, transfer_number, row.number))
            else:
                # Unique — just update season_id
                updates.append((row.pt_id, correct_sid))

        # Summary
        print("=" * 70)
        print("SUMMARY")
        print("=" * 70)
        print(f"  Total wrong records:    {len(wrong_rows)}")
        print(f"  → Will UPDATE:          {len(updates)}")
        print(f"  → Will DELETE (dupes):  {len(deletes)}")
        transfer_count = sum(1 for _, _, t, _ in deletes if t)
        print(f"    → with number transfer: {transfer_count}")
        print(f"  → Skipped:              {len(skipped)}")
        print()

        # Detail: updates
        if updates:
            print("-" * 70)
            print(f"UPDATES ({len(updates)})")
            print("-" * 70)
            for pt_id, correct_sid in updates[:20]:
                print(f"  pt.id={pt_id} → season_id={correct_sid}")
            if len(updates) > 20:
                print(f"  ... and {len(updates) - 20} more")
            print()

        # Detail: deletes
        if deletes:
            print("-" * 70)
            print(f"DELETES ({len(deletes)})")
            print("-" * 70)
            for pt_id, existing_id, transfer, number in deletes[:20]:
                note = f" (transfer number={number} to pt.id={existing_id})" if transfer else ""
                print(f"  DELETE pt.id={pt_id}, keep pt.id={existing_id}{note}")
            if len(deletes) > 20:
                print(f"  ... and {len(deletes) - 20} more")
            print()

        # Detail: skipped
        if skipped:
            print("-" * 70)
            print(f"SKIPPED ({len(skipped)})")
            print("-" * 70)
            for pt_id, reason in skipped:
                print(f"  pt.id={pt_id}: {reason}")
            print()

        if not apply:
            print("*** DRY RUN — no changes made. Use --apply to execute. ***")
            return

        # Step 4: Apply changes in a single transaction
        print("Applying changes...")

        # Transfer numbers first
        for pt_id, existing_id, transfer, number in deletes:
            if transfer:
                await session.execute(text(
                    "UPDATE player_teams SET number = :num WHERE id = :id"
                ), {"num": number, "id": existing_id})

        # Delete duplicates
        if deletes:
            delete_ids = [pt_id for pt_id, _, _, _ in deletes]
            await session.execute(text(
                "DELETE FROM player_teams WHERE id = ANY(:ids)"
            ), {"ids": delete_ids})

        # Update season_id
        for pt_id, correct_sid in updates:
            await session.execute(text(
                "UPDATE player_teams SET season_id = :sid WHERE id = :id"
            ), {"sid": correct_sid, "id": pt_id})

        await session.commit()
        print(f"Done. {len(updates)} updated, {len(deletes)} deleted, {transfer_count} numbers transferred.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fix wrong season_id in player_teams")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--dry-run", action="store_true", default=True, help="Preview changes (default)")
    group.add_argument("--apply", action="store_true", help="Apply changes")
    args = parser.parse_args()
    asyncio.run(main(apply=args.apply))
