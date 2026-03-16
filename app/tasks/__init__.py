from celery import Celery
from celery.schedules import crontab
from celery.signals import worker_shutdown

from app.config import get_settings

settings = get_settings()

celery_app = Celery(
    "qfl_tasks",
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=["app.tasks.sync_tasks", "app.tasks.live_tasks", "app.tasks.weather_tasks", "app.tasks.ticket_tasks"],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Asia/Almaty",
    enable_utc=True,
    task_ignore_result=True,
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
        "sync-extended-stats-every-6h": {
            "task": "app.tasks.sync_tasks.sync_extended_stats",
            "schedule": crontab(minute="15", hour="*/6"),
        },
        "check-tour-completion-every-30min": {
            "task": "app.tasks.sync_tasks.check_tour_completion",
            "schedule": crontab(minute="*/30"),
        },
    }
else:
    celery_app.conf.beat_schedule = {}

celery_app.conf.beat_schedule["fetch-weather-every-3h"] = {
    "task": "app.tasks.weather_tasks.fetch_weather",
    "schedule": crontab(minute="30", hour="*/3"),
}

celery_app.conf.beat_schedule["search-tickets-every-3h"] = {
    "task": "app.tasks.ticket_tasks.search_tickets",
    "schedule": crontab(minute="0", hour="*/3"),
}

@worker_shutdown.connect
def on_worker_shutdown(**kwargs):
    from app.utils.async_celery import cleanup_event_loop
    cleanup_event_loop()
