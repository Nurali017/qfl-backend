"""Helpers for caching public season API responses."""

from app.utils.cache import cache_delete

SEASONS_LIST_CACHE_KEY = "seasons:list"


def season_detail_cache_key(season_id: int) -> str:
    return f"seasons:detail:{season_id}"


def invalidate_season_api_cache(season_id: int | None = None) -> None:
    cache_delete(SEASONS_LIST_CACHE_KEY)
    if season_id is not None:
        cache_delete(season_detail_cache_key(season_id))
