from celery import Celery
from celery.schedules import crontab
from celery.signals import worker_shutdown

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
            "schedule": crontab(minute="*/2"),
        },
        "sync-live-events": {
            "task": "app.tasks.live_tasks.sync_live_game_events",
            "schedule": 15.0,
        },
        "auto-end-finished-games": {
            "task": "app.tasks.live_tasks.auto_end_finished_games",
            "schedule": crontab(minute="*/5"),
        },
    }
else:
    celery_app.conf.beat_schedule = {}


@worker_shutdown.connect
def on_worker_shutdown(**kwargs):
    from app.utils.async_celery import cleanup_event_loop
    cleanup_event_loop()
