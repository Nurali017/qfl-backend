"""YouTube view_count overview API."""

from __future__ import annotations

from datetime import date, datetime

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.models import Game
from app.models.media_video import MediaVideo

router = APIRouter(prefix="/youtube-stats", tags=["youtube-stats"])


class GameViewRow(BaseModel):
    game_id: int
    date: date
    tour: int | None
    season_id: int | None
    home_team: str | None
    away_team: str | None
    stage_name: str | None
    youtube_live_url: str | None
    youtube_live_view_count: int | None
    video_review_url: str | None
    video_review_view_count: int | None
    total_view_count: int
    youtube_stats_updated_at: datetime | None


class MediaVideoRow(BaseModel):
    id: int
    title: str
    youtube_id: str
    view_count: int | None
    stats_updated_at: datetime | None


class Summary(BaseModel):
    total_live_views: int
    total_review_views: int
    total_media_views: int
    games_with_live_url: int
    games_with_review_url: int
    media_videos_count: int


class TourRow(BaseModel):
    tour: int | None
    stage_name: str | None
    games_count: int
    total_live_views: int
    total_review_views: int
    total_views: int


class TeamRow(BaseModel):
    team_id: int
    team_name: str
    games_count: int
    total_live_views: int
    total_review_views: int
    total_views: int


class YoutubeStatsOverview(BaseModel):
    summary: Summary
    games: list[GameViewRow]
    by_tour: list[TourRow]
    by_team: list[TeamRow]
    media_videos: list[MediaVideoRow]


@router.get("/overview", response_model=YoutubeStatsOverview)
async def get_youtube_stats_overview(
    season_id: int | None = Query(None),
    db: AsyncSession = Depends(get_db),
) -> YoutubeStatsOverview:
    """Internal page data: games with YouTube URLs + active media_videos with view_count."""
    stmt = (
        select(Game)
        .where(or_(Game.youtube_live_url.is_not(None), Game.video_review_url.is_not(None)))
        .options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
            selectinload(Game.stage),
        )
    )
    if season_id is not None:
        stmt = stmt.where(Game.season_id == season_id)

    games: list[Game] = list((await db.execute(stmt)).scalars().all())

    rows: list[GameViewRow] = []
    total_live = 0
    total_review = 0
    games_with_live = 0
    games_with_review = 0

    for g in games:
        live_views = g.youtube_live_view_count or 0
        review_views = g.video_review_view_count or 0
        if g.youtube_live_url:
            games_with_live += 1
            total_live += live_views
        if g.video_review_url:
            games_with_review += 1
            total_review += review_views

        rows.append(
            GameViewRow(
                game_id=g.id,
                date=g.date,
                tour=g.tour,
                season_id=g.season_id,
                home_team=g.home_team.name if g.home_team else None,
                away_team=g.away_team.name if g.away_team else None,
                stage_name=g.stage.name if g.stage else None,
                youtube_live_url=g.youtube_live_url,
                youtube_live_view_count=g.youtube_live_view_count,
                video_review_url=g.video_review_url,
                video_review_view_count=g.video_review_view_count,
                total_view_count=live_views + review_views,
                youtube_stats_updated_at=g.youtube_stats_updated_at,
            )
        )

    rows.sort(key=lambda r: r.total_view_count, reverse=True)

    # Aggregate by tour
    tour_agg: dict[tuple[int | None, str | None], dict] = {}
    for g in games:
        key = (g.tour, g.stage.name if g.stage else None)
        agg = tour_agg.setdefault(
            key,
            {"games_count": 0, "live": 0, "review": 0},
        )
        agg["games_count"] += 1
        agg["live"] += g.youtube_live_view_count or 0
        agg["review"] += g.video_review_view_count or 0

    tour_rows = [
        TourRow(
            tour=k[0],
            stage_name=k[1],
            games_count=v["games_count"],
            total_live_views=v["live"],
            total_review_views=v["review"],
            total_views=v["live"] + v["review"],
        )
        for k, v in tour_agg.items()
    ]
    tour_rows.sort(key=lambda r: r.total_views, reverse=True)

    # Aggregate by team (team appears in both home and away games)
    team_agg: dict[int, dict] = {}
    for g in games:
        live = g.youtube_live_view_count or 0
        review = g.video_review_view_count or 0
        for team in (g.home_team, g.away_team):
            if team is None:
                continue
            agg = team_agg.setdefault(
                team.id,
                {"team_name": team.name, "games_count": 0, "live": 0, "review": 0},
            )
            agg["games_count"] += 1
            agg["live"] += live
            agg["review"] += review

    team_rows = [
        TeamRow(
            team_id=team_id,
            team_name=v["team_name"],
            games_count=v["games_count"],
            total_live_views=v["live"],
            total_review_views=v["review"],
            total_views=v["live"] + v["review"],
        )
        for team_id, v in team_agg.items()
    ]
    team_rows.sort(key=lambda r: r.total_views, reverse=True)

    media_stmt = select(MediaVideo).where(MediaVideo.is_active.is_(True))
    media_items = list((await db.execute(media_stmt)).scalars().all())
    total_media = sum((m.view_count or 0) for m in media_items)

    media_rows = [
        MediaVideoRow(
            id=m.id,
            title=m.title,
            youtube_id=m.youtube_id,
            view_count=m.view_count,
            stats_updated_at=m.stats_updated_at,
        )
        for m in sorted(media_items, key=lambda m: m.view_count or 0, reverse=True)
    ]

    return YoutubeStatsOverview(
        summary=Summary(
            total_live_views=total_live,
            total_review_views=total_review,
            total_media_views=total_media,
            games_with_live_url=games_with_live,
            games_with_review_url=games_with_review,
            media_videos_count=len(media_items),
        ),
        games=rows,
        by_tour=tour_rows,
        by_team=team_rows,
        media_videos=media_rows,
    )
