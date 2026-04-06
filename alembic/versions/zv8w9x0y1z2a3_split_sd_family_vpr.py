"""Split SD Family / VPR Astana: create new VPR team, restore SD Family, add cup tech defeat.

Team 645 was SD Family (1L 2025) but got renamed to VPR Astana when VPR replaced
SD Family in 2L 2026. This migration creates a proper new VPR Astana team,
transfers all 2026 season data to it, restores team 645 as SD Family,
and adds SD Family to Cup 2026 with a 0:3 technical defeat vs Иртыш.

Revision ID: zv8w9x0y1z2a3
Revises: zu7v8w9x0y1z2
Create Date: 2026-04-06
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "zv8w9x0y1z2a3"
down_revision = "zu7v8w9x0y1z2"
branch_labels = None
depends_on = None

# Constants
OLD_TEAM_ID = 645          # Currently "VPR Astana", originally SD Family
SD_FAMILY_SEASON_IDS = []  # Old seasons stay with SD Family (auto-detected)
SEASON_2L_2026 = 203
SEASON_CUP_2026 = 202
CUP_STAGE_ID = 151         # 1/16 финала
IRTISH_TEAM_ID = 595       # Иртыш


def upgrade() -> None:
    bind = op.get_bind()

    # -----------------------------------------------------------------------
    # 0. Clear fcms_team_id from team 645 first (unique constraint)
    # -----------------------------------------------------------------------
    bind.execute(sa.text(
        "UPDATE teams SET fcms_team_id = NULL WHERE id = :old"
    ), {"old": OLD_TEAM_ID})

    # -----------------------------------------------------------------------
    # 1. Create new VPR Astana team
    # -----------------------------------------------------------------------
    new_vpr_id = bind.execute(
        sa.text(
            "INSERT INTO teams (name, name_kz, name_en, logo_url, fcms_team_id, updated_at) "
            "VALUES ('VPR Astana', 'VPR Astana', 'VPR Astana', "
            "        'public/team-logos/vpr-astana.png', 14417, NOW()) "
            "RETURNING id"
        )
    ).scalar_one()

    # -----------------------------------------------------------------------
    # 2. Transfer season 203 data from team 645 → new VPR
    # -----------------------------------------------------------------------
    params = {"vpr": new_vpr_id, "old": OLD_TEAM_ID, "s203": SEASON_2L_2026}

    # Core tables with season_id
    bind.execute(sa.text(
        "UPDATE season_participants SET team_id = :vpr "
        "WHERE team_id = :old AND season_id = :s203"
    ), params)

    bind.execute(sa.text(
        "UPDATE games SET home_team_id = :vpr "
        "WHERE home_team_id = :old AND season_id = :s203"
    ), params)

    bind.execute(sa.text(
        "UPDATE games SET away_team_id = :vpr "
        "WHERE away_team_id = :old AND season_id = :s203"
    ), params)

    bind.execute(sa.text(
        "UPDATE player_teams SET team_id = :vpr "
        "WHERE team_id = :old AND season_id = :s203"
    ), params)

    bind.execute(sa.text(
        "UPDATE score_table SET team_id = :vpr "
        "WHERE team_id = :old AND season_id = :s203"
    ), params)

    bind.execute(sa.text(
        "UPDATE player_season_stats SET team_id = :vpr "
        "WHERE team_id = :old AND season_id = :s203"
    ), params)

    # Stats/events tables — filter by games in season 203
    for table in ("game_team_stats", "game_events", "game_lineups", "game_player_stats"):
        bind.execute(sa.text(
            f"UPDATE {table} SET team_id = :vpr "
            f"WHERE team_id = :old "
            f"AND game_id IN (SELECT id FROM games WHERE season_id = :s203)"
        ), params)

    # -----------------------------------------------------------------------
    # 3. Restore team 645 as SD Family
    # -----------------------------------------------------------------------
    bind.execute(sa.text(
        "UPDATE teams SET "
        "  name = 'SD Family', "
        "  name_kz = 'SD Family', "
        "  name_en = 'SD Family', "
        "  logo_url = 'public/team-logos/sd-family-v4.webp', "
        "  fcms_team_id = NULL, "
        "  updated_at = NOW() "
        "WHERE id = :old"
    ), {"old": OLD_TEAM_ID})

    # -----------------------------------------------------------------------
    # 4. Cup: SD Family withdrawn — add to bracket with no score
    # -----------------------------------------------------------------------
    bind.execute(sa.text(
        "INSERT INTO season_participants (team_id, season_id, is_disqualified, sort_order) "
        "VALUES (:tid, :sid, true, 99)"
    ), {"tid": OLD_TEAM_ID, "sid": SEASON_CUP_2026})

    bind.execute(sa.text(
        "INSERT INTO games ("
        "  season_id, tour, stage_id, home_team_id, away_team_id, "
        "  date, time, home_score, away_score, "
        "  status, has_stats, has_lineup, is_schedule_tentative, updated_at"
        ") VALUES ("
        "  :sid, 1, :stage, :home, :away, "
        "  '2026-04-09', '14:00', NULL, NULL, "
        "  'technical_defeat', false, false, false, NOW()"
        ")"
    ), {"sid": SEASON_CUP_2026, "stage": CUP_STAGE_ID,
        "home": OLD_TEAM_ID, "away": IRTISH_TEAM_ID})


def downgrade() -> None:
    bind = op.get_bind()

    # Remove cup game and participant
    bind.execute(sa.text(
        "DELETE FROM games "
        "WHERE season_id = :sid AND home_team_id = :home AND away_team_id = :away "
        "AND status = 'technical_defeat'"
    ), {"sid": SEASON_CUP_2026, "home": OLD_TEAM_ID, "away": IRTISH_TEAM_ID})
    bind.execute(sa.text(
        "DELETE FROM season_participants "
        "WHERE team_id = :tid AND season_id = :sid"
    ), {"tid": OLD_TEAM_ID, "sid": SEASON_CUP_2026})

    # Find the VPR Astana team we created (has fcms_team_id=14417, is NOT team 645)
    new_vpr_id = bind.execute(sa.text(
        "SELECT id FROM teams "
        "WHERE fcms_team_id = 14417 AND id != :old "
        "ORDER BY id DESC LIMIT 1"
    ), {"old": OLD_TEAM_ID}).scalar_one_or_none()

    if new_vpr_id is None:
        return

    params = {"vpr": new_vpr_id, "old": OLD_TEAM_ID, "s203": SEASON_2L_2026}

    # Restore team 645 as VPR Astana
    bind.execute(sa.text(
        "UPDATE teams SET "
        "  name = 'VPR Astana', "
        "  name_kz = 'VPR Astana', "
        "  name_en = 'VPR Astana', "
        "  logo_url = 'public/team-logos/vpr-astana.png', "
        "  fcms_team_id = 14417, "
        "  updated_at = NOW() "
        "WHERE id = :old"
    ), {"old": OLD_TEAM_ID})

    # Transfer data back: new VPR → team 645
    bind.execute(sa.text(
        "UPDATE season_participants SET team_id = :old "
        "WHERE team_id = :vpr AND season_id = :s203"
    ), params)

    bind.execute(sa.text(
        "UPDATE games SET home_team_id = :old "
        "WHERE home_team_id = :vpr AND season_id = :s203"
    ), params)

    bind.execute(sa.text(
        "UPDATE games SET away_team_id = :old "
        "WHERE away_team_id = :vpr AND season_id = :s203"
    ), params)

    bind.execute(sa.text(
        "UPDATE player_teams SET team_id = :old "
        "WHERE team_id = :vpr AND season_id = :s203"
    ), params)

    bind.execute(sa.text(
        "UPDATE score_table SET team_id = :old "
        "WHERE team_id = :vpr AND season_id = :s203"
    ), params)

    bind.execute(sa.text(
        "UPDATE player_season_stats SET team_id = :old "
        "WHERE team_id = :vpr AND season_id = :s203"
    ), params)

    for table in ("game_team_stats", "game_events", "game_lineups", "game_player_stats"):
        bind.execute(sa.text(
            f"UPDATE {table} SET team_id = :old "
            f"WHERE team_id = :vpr "
            f"AND game_id IN (SELECT id FROM games WHERE season_id = :s203)"
        ), params)

    # Delete new VPR team
    bind.execute(sa.text("DELETE FROM teams WHERE id = :vpr"), {"vpr": new_vpr_id})
