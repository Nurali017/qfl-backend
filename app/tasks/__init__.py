from celery import Celery
from celery.schedules import crontab

from app.config import get_settings

settings = get_settings()

celery_app = Celery(
    "qfl_tasks",
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=["app.tasks.sync_tasks", "app.tasks.live_tasks"],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Asia/Almaty",
    enable_utc=True,
)

if settings.sota_enabled:
    celery_app.conf.beat_schedule = {
        "sync-references-daily": {
            "task": "app.tasks.sync_tasks.sync_references",
            "schedule": crontab(hour=6, minute=0),
        },
        "sync-games-every-2h": {
            "task": "app.tasks.sync_tasks.sync_games",
            "schedule": crontab(minute=0, hour="*/2"),
        },
        "sync-live-stats-every-15min": {
            "task": "app.tasks.sync_tasks.sync_live_stats",
            "schedule": crontab(minute="*/15"),
        },
        # Live match tasks
        "check-upcoming-games-every-5min": {
            "task": "app.tasks.live_tasks.check_upcoming_games",
            "schedule": crontab(minute="*/5"),
        },
        "sync-live-events-every-30sec": {
            "task": "app.tasks.live_tasks.sync_live_game_events",
            "schedule": 30.0,  # Every 30 seconds
        },
        "start-games-by-schedule-every-min": {
            "task": "app.tasks.live_tasks.start_game_by_schedule",
            "schedule": crontab(minute="*"),  # Every minute
        },
        "end-finished-games-every-10min": {
            "task": "app.tasks.live_tasks.end_finished_games",
            "schedule": crontab(minute="*/10"),
        },
    }
else:
    celery_app.conf.beat_schedule = {}
