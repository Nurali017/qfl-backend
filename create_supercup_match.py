"""
Create Super Cup fixture (Qairat vs Tobol) for 2026 season.

Default mode is dry-run. Use --apply to commit changes.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import date, time

from sqlalchemy import and_, func, or_, select

from app.database import AsyncSessionLocal
from app.models.game import Game
from app.models.season import Season
from app.models.stage import Stage
from app.models.stadium import Stadium
from app.models.team import Team

SEASON_ID = 200
HOME_TEAM_ID = 13
AWAY_TEAM_ID = 90
STADIUM_ID = 10

MATCH_DATE = date(2026, 2, 28)
MATCH_TIME = time(17, 0)
STADIUM_NAME = "СК «Астана Арена»"
STAGE_NAME_RU = "Суперкубок"
STAGE_NAME_KZ = "Суперкубок"
STAGE_NAME_EN = "Super Cup"
TICKET_URL = "https://afisha.yandex.kz/astana/sport/football-kairat-tobyl"


async def create_supercup_match(apply: bool) -> None:
    if len(TICKET_URL) > 500:
        raise ValueError("ticket_url must be <= 500 characters")

    async with AsyncSessionLocal() as session:
        season = await session.scalar(select(Season).where(Season.id == SEASON_ID))
        if season is None:
            raise ValueError(f"Season {SEASON_ID} not found")

        home_team = await session.scalar(select(Team).where(Team.id == HOME_TEAM_ID))
        if home_team is None:
            raise ValueError(f"Home team {HOME_TEAM_ID} not found")

        away_team = await session.scalar(select(Team).where(Team.id == AWAY_TEAM_ID))
        if away_team is None:
            raise ValueError(f"Away team {AWAY_TEAM_ID} not found")

        stadium = await session.scalar(select(Stadium).where(Stadium.id == STADIUM_ID))
        if stadium is None:
            raise ValueError(f"Stadium {STADIUM_ID} not found")

        duplicate_match = await session.scalar(
            select(Game)
            .where(Game.date == MATCH_DATE)
            .where(
                or_(
                    and_(Game.home_team_id == HOME_TEAM_ID, Game.away_team_id == AWAY_TEAM_ID),
                    and_(Game.home_team_id == AWAY_TEAM_ID, Game.away_team_id == HOME_TEAM_ID),
                )
            )
            .order_by(Game.id.desc())
            .limit(1)
        )
        if duplicate_match is not None:
            print(
                json.dumps(
                    {
                        "status": "already_exists",
                        "dry_run": not apply,
                        "game_id": duplicate_match.id,
                        "stage_id": duplicate_match.stage_id,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return

        stage = await session.scalar(
            select(Stage)
            .where(Stage.season_id == SEASON_ID)
            .where(
                or_(
                    func.lower(Stage.name) == STAGE_NAME_RU.lower(),
                    func.lower(func.coalesce(Stage.name_kz, "")) == STAGE_NAME_KZ.lower(),
                    func.lower(func.coalesce(Stage.name_en, "")) == STAGE_NAME_EN.lower(),
                )
            )
            .order_by(Stage.id.desc())
            .limit(1)
        )

        would_create_stage = stage is None
        created_stage_id: int | None = None
        if stage is None and apply:
            stage = Stage(
                season_id=SEASON_ID,
                name=STAGE_NAME_RU,
                name_kz=STAGE_NAME_KZ,
                name_en=STAGE_NAME_EN,
                stage_number=None,
                sort_order=0,
            )
            session.add(stage)
            await session.flush()
            created_stage_id = stage.id

        if stage is None:
            stage_name = STAGE_NAME_RU
            stage_id = None
        else:
            stage_name = stage.name
            stage_id = stage.id

        if not apply:
            print(
                json.dumps(
                    {
                        "status": "dry_run",
                        "dry_run": True,
                        "would_create_stage": would_create_stage,
                        "stage_id": stage_id,
                        "stage_name": stage_name,
                        "would_create_match": {
                            "season_id": SEASON_ID,
                            "stage_id": stage_id,
                            "date": MATCH_DATE.isoformat(),
                            "time": MATCH_TIME.isoformat(),
                            "home_team_id": HOME_TEAM_ID,
                            "away_team_id": AWAY_TEAM_ID,
                            "stadium_id": STADIUM_ID,
                            "stadium": STADIUM_NAME,
                            "ticket_url": TICKET_URL,
                        },
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return

        game = Game(
            season_id=SEASON_ID,
            stage_id=stage_id,
            date=MATCH_DATE,
            time=MATCH_TIME,
            home_team_id=HOME_TEAM_ID,
            away_team_id=AWAY_TEAM_ID,
            stadium_id=STADIUM_ID,
            stadium=STADIUM_NAME,
            ticket_url=TICKET_URL,
        )
        session.add(game)
        await session.flush()
        created_game_id = game.id

        await session.commit()

        print(
            json.dumps(
                {
                    "status": "created",
                    "dry_run": False,
                    "created_stage_id": created_stage_id,
                    "created_game_id": created_game_id,
                    "stage_id": stage_id,
                },
                ensure_ascii=False,
                indent=2,
            )
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create Super Cup 2026 fixture (Qairat vs Tobol)."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes. Without this flag script runs in dry-run mode.",
    )
    args = parser.parse_args()
    asyncio.run(create_supercup_match(apply=args.apply))


if __name__ == "__main__":
    main()
