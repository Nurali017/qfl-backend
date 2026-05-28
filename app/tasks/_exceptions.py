"""Shared exception types raised from Celery tasks."""


class LiveGamesActiveSkip(Exception):
    """Raised when a task defers itself because live games are in progress.

    The wrapper task catches this via Celery's retry mechanism (self.retry).
    Carries the live_count so retry logs are diagnosable.
    """

    def __init__(self, live_count: int):
        self.live_count = live_count
        super().__init__(f"{live_count} live game(s) in progress")
