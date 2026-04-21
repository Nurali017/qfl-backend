import logging

from celery import Celery
from celery.schedules import crontab
from celery.signals import worker_ready, worker_shutdown

from app.config import get_settings
from app.utils.feature_flags import log_feature_flags

logger = logging.getLogger(__name__)

settings = get_settings()

celery_app = Celery(
    "qfl_tasks",
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=[
        "app.tasks.sync_tasks",
        "app.tasks.live_tasks",
        "app.tasks.weather_tasks",
        "app.tasks.ticket_tasks",
        "app.tasks.fcms_tasks",
        "app.tasks.youtube_tasks",
        "app.tasks.telegram_tasks",
        "app.tasks.goal_video_tasks",
    ],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Asia/Almaty",
    enable_utc=True,
    task_ignore_result=True,
    task_soft_time_limit=600,
    task_time_limit=660,
    task_routes={
        "app.tasks.goal_video_tasks.sync_goal_videos_task": {"queue": "media"},
        "app.tasks.live_tasks.sync_live_game_events": {"queue": "live"},
        "app.tasks.live_tasks.sync_single_game": {"queue": "live"},
        "app.tasks.live_tasks.auto_start_live_games": {"queue": "live"},
        "app.tasks.live_tasks.auto_end_finished_games": {"queue": "live"},
        "app.tasks.live_tasks.fetch_pregame_lineups": {"queue": "live"},
        "app.tasks.telegram_tasks.post_match_start_task": {"queue": "telegram"},
        "app.tasks.telegram_tasks.post_match_finish_task": {"queue": "telegram"},
        "app.tasks.telegram_tasks.post_game_event_task": {"queue": "telegram"},
        "app.tasks.telegram_tasks.post_pregame_lineup_task": {"queue": "telegram"},
        "app.tasks.telegram_tasks.post_goal_video_task": {"queue": "media"},
        "app.tasks.telegram_tasks.tour_announce_daily": {"queue": "telegram"},
    },
)

if settings.sota_enabled:
    celery_app.conf.beat_schedule = {
        "sync-best-players-every-15min": {
            "task": "app.tasks.sync_tasks.sync_best_players",
            "schedule": crontab(minute="*/15"),
        },
        "auto-start-live-games": {
            "task": "app.tasks.live_tasks.auto_start_live_games",
            "schedule": crontab(minute="*/1"),
        },
        "sync-live-events": {
            "task": "app.tasks.live_tasks.sync_live_game_events",
            "schedule": 15.0,
        },
        "auto-end-finished-games": {
            "task": "app.tasks.live_tasks.auto_end_finished_games",
            "schedule": crontab(minute="*/3"),
        },
        "sync-post-match-protocol": {
            "task": "app.tasks.live_tasks.sync_post_match_protocol",
            "schedule": crontab(minute="*/30"),
        },
        "fetch-pregame-lineups": {
            "task": "app.tasks.live_tasks.fetch_pregame_lineups",
            "schedule": crontab(minute="*/3"),
        },
        "sync-extended-stats-nightly": {
            "task": "app.tasks.sync_tasks.sync_extended_stats",
            "schedule": crontab(minute="15", hour="3"),
        },
        "retry-missing-team-of-week-every-6h": {
            "task": "app.tasks.sync_tasks.retry_missing_team_of_week",
            "schedule": crontab(minute="45", hour="*/6"),
        },
    }
else:
    celery_app.conf.beat_schedule = {}

celery_app.conf.beat_schedule["fetch-weather-every-3h"] = {
    "task": "app.tasks.weather_tasks.fetch_weather",
    "schedule": crontab(minute="30", hour="*/3"),
}

if settings.telegram_public_posts_enabled and settings.telegram_tour_announce_enabled:
    celery_app.conf.beat_schedule["tour-announce-daily-21-ala"] = {
        "task": "app.tasks.telegram_tasks.tour_announce_daily",
        "schedule": crontab(minute="0", hour="21"),
    }

celery_app.conf.beat_schedule["search-tickets-twice-daily"] = {
    "task": "app.tasks.ticket_tasks.search_tickets",
    "schedule": crontab(minute="0", hour="9,18"),
}

if settings.fcms_enabled:
    celery_app.conf.beat_schedule["fetch-fcms-pregame-lineups"] = {
        "task": "app.tasks.fcms_tasks.fetch_fcms_pregame_lineups",
        "schedule": crontab(minute="*/20"),
    }
    celery_app.conf.beat_schedule["sync-fcms-post-match-protocol"] = {
        "task": "app.tasks.fcms_tasks.sync_fcms_post_match_protocol",
        "schedule": crontab(minute="*/15"),
    }
    celery_app.conf.beat_schedule["fcms-bulk-import-daily"] = {
        "task": "app.tasks.fcms_tasks.fcms_bulk_import",
        "schedule": crontab(minute="0", hour="11,18"),
    }
    celery_app.conf.beat_schedule["sync-fcms-rosters-daily"] = {
        "task": "app.tasks.fcms_tasks.sync_fcms_rosters",
        "schedule": crontab(minute="0", hour="7,10,14,17"),
    }

if settings.youtube_auto_link_enabled:
    celery_app.conf.beat_schedule["link-youtube-videos-every-30min"] = {
        "task": "app.tasks.youtube_tasks.link_youtube_videos",
        "schedule": crontab(minute="*/30"),
    }

if settings.google_drive_enabled:
    _gv_interval = max(1, settings.goal_video_sync_interval_minutes)
    celery_app.conf.beat_schedule["goal-video-sync"] = {
        "task": "app.tasks.goal_video_tasks.sync_goal_videos_task",
        "schedule": crontab(minute=f"*/{_gv_interval}"),
    }

if settings.youtube_api_key:
    celery_app.conf.beat_schedule["youtube-view-counts-live-1min"] = {
        "task": "app.tasks.youtube_tasks.sync_view_counts_live",
        "schedule": crontab(minute="*/1"),
    }
    celery_app.conf.beat_schedule["youtube-view-counts-fresh-5min"] = {
        "task": "app.tasks.youtube_tasks.sync_view_counts_fresh",
        "schedule": crontab(minute="*/5"),
    }
    celery_app.conf.beat_schedule["youtube-view-counts-medium-hourly"] = {
        "task": "app.tasks.youtube_tasks.sync_view_counts_medium",
        "schedule": crontab(minute="0"),
    }
    celery_app.conf.beat_schedule["youtube-view-counts-old-daily"] = {
        "task": "app.tasks.youtube_tasks.sync_view_counts_old",
        "schedule": crontab(hour="4", minute="0"),
    }
    celery_app.conf.beat_schedule["youtube-view-counts-media-30min"] = {
        "task": "app.tasks.youtube_tasks.sync_view_counts_media",
        "schedule": crontab(minute="*/30"),
    }

@worker_ready.connect
def on_worker_ready(sender=None, **kwargs):
    queues = sorted(q.name for q in sender.app.amqp.queues.consume_from.values()) if sender else []
    logger.info("celery_worker_ready queues=%s", queues)
    log_feature_flags(logger, service="celery_worker")


@worker_shutdown.connect
def on_worker_shutdown(**kwargs):
    from app.utils.async_celery import cleanup_event_loop
    cleanup_event_loop()
