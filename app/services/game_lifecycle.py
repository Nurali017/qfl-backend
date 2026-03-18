"""
Game lifecycle service — single source of truth for match state transitions.

All status changes MUST go through this service. Direct PATCH of status is deprecated.
"""
import logging
from datetime import timedelta

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Game, GameStatus
from app.utils.timestamps import ensure_utc, utcnow

logger = logging.getLogger(__name__)

# Allowed state transitions: current_status → {allowed targets}
TRANSITIONS: dict[GameStatus, set[GameStatus]] = {
    GameStatus.created: {GameStatus.live, GameStatus.postponed, GameStatus.cancelled},
    GameStatus.live: {
        GameStatus.finished,
        GameStatus.postponed,
        GameStatus.cancelled,
        GameStatus.technical_defeat,
    },
    GameStatus.finished: {GameStatus.created},  # reset only
}

VALID_ACTIONS = {
    "start_live",
    "finish_live",
    "start_second_half",
    "reset_to_created",
    "set_postponed",
    "set_cancelled",
    "set_technical_defeat",
}


class InvalidTransition(Exception):
    """Raised when a state transition is not allowed."""

    def __init__(self, game_id: int, current: GameStatus, action: str):
        self.game_id = game_id
        self.current = current
        self.action = action
        super().__init__(
            f"Game {game_id}: action '{action}' not allowed from status '{current.value}'"
        )


class GameLifecycleService:
    """Manages match state transitions with row-level locking."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def _lock_game(self, game_id: int) -> Game:
        """SELECT ... FOR UPDATE on the game row."""
        result = await self.db.execute(
            select(Game).where(Game.id == game_id).with_for_update()
        )
        game = result.scalar_one_or_none()
        if not game:
            raise ValueError(f"Game {game_id} not found")
        return game

    def _validate_transition(
        self, game: Game, target: GameStatus, action: str
    ) -> None:
        allowed = TRANSITIONS.get(game.status, set())
        if target not in allowed:
            raise InvalidTransition(game.id, game.status, action)

    # ------------------------------------------------------------------ #
    #  Actions                                                            #
    # ------------------------------------------------------------------ #

    async def start_live(self, game_id: int) -> dict:
        """created -> live.

        Lock covers ONLY the status transition.
        Lineup sync and initial event sync run after the lock is released
        (they call db.commit() internally which would break FOR UPDATE).
        """
        from app.services.live_sync_service import LiveSyncService
        from app.services.sota_client import get_sota_client
        from app.utils.live_flag import set_live_flag

        # 1. Atomic transition under row lock
        game = await self._lock_game(game_id)
        self._validate_transition(game, GameStatus.live, "start_live")
        needs_lineup = not game.has_lineup and game.sota_id and not game.sync_disabled
        can_sync = bool(game.sota_id) and not game.sync_disabled
        game.status = GameStatus.live
        game.half1_started_at = utcnow()
        game.live_phase = "in_progress"
        await self.db.commit()  # releases FOR UPDATE lock
        await set_live_flag()

        # 2. Best-effort sync (outside lock — these have their own commits)
        if can_sync:
            if needs_lineup:
                try:
                    svc = LiveSyncService(self.db, get_sota_client())
                    await svc.sync_pregame_lineup(game_id)
                except Exception:
                    logger.warning("Pre-game lineup sync failed for game %s", game_id)
            try:
                svc = LiveSyncService(self.db, get_sota_client())
                r = await svc.sync_live_events(game_id)
                return {"game_id": game_id, "action": "start_live", "new_events": r.get("added", 0)}
            except Exception:
                logger.warning("Initial event sync failed for game %s", game_id)

        return {"game_id": game_id, "action": "start_live", "new_events": 0}

    async def finish_live(self, game_id: int) -> dict:
        """
        live -> finished (normal path).

        Idempotent: already finished + finished_at filled -> success/no-op.
        Repair-tail: finished + finished_at NULL -> fill it, clean live fields,
                     enqueue follow-up, NO repeated SOTA final-sync.

        Sync runs BEFORE acquiring the row lock because sync methods call
        db.commit() internally (which would release FOR UPDATE).  The lock
        then covers only the atomic status transition.  If a concurrent call
        finishes first, the idempotent check inside the lock handles it.
        """
        from app.services.live_sync_service import LiveSyncService
        from app.services.sota_client import get_sota_client
        from app.utils.live_flag import clear_live_flag

        # 1. Pre-lock: quick read to decide whether to sync
        pre = await self.db.get(Game, game_id)
        if not pre:
            raise ValueError(f"Game {game_id} not found")

        # Final SOTA sync (before lock — these have their own commits)
        if pre.status == GameStatus.live and pre.sota_id and not pre.sync_disabled:
            try:
                svc = LiveSyncService(self.db, get_sota_client())
                await svc.sync_live_events(game_id)
                await svc.sync_live_stats(game_id)
                await svc.sync_live_player_stats(game_id)
            except Exception:
                logger.exception("Final sync failed for game %s", game_id)

        # 2. Atomic transition under row lock
        game = await self._lock_game(game_id)

        # --- idempotent ---
        if game.status == GameStatus.finished and game.finished_at is not None:
            return {"game_id": game_id, "action": "finish_live", "noop": True}

        # --- repair-tail (the 16-Mar incident path) ---
        if game.status == GameStatus.finished and game.finished_at is None:
            game.finished_at = ensure_utc(game.updated_at) or utcnow()
            game.live_minute = None
            game.live_half = None
            game.live_phase = None
            await self.db.commit()
            self._enqueue_post_finish(game)
            return {"game_id": game_id, "action": "finish_live", "repair_tail": True}

        # --- normal path: live -> finished ---
        self._validate_transition(game, GameStatus.finished, "finish_live")

        game.status = GameStatus.finished
        game.finished_at = utcnow()
        game.live_minute = None
        game.live_half = None
        game.live_phase = None
        await self.db.commit()  # releases FOR UPDATE lock

        # 3. Post-commit cleanup (outside lock)
        remaining = await self.db.execute(
            select(func.count()).select_from(Game).where(
                Game.status == GameStatus.live
            )
        )
        if remaining.scalar() == 0:
            await clear_live_flag()

        self._enqueue_post_finish(game)
        return {"game_id": game_id, "action": "finish_live"}

    async def start_second_half(self, game_id: int) -> dict:
        """Mark second half start. Game must be live."""
        game = await self._lock_game(game_id)
        if game.status != GameStatus.live:
            raise InvalidTransition(game.id, game.status, "start_second_half")

        game.half2_started_at = utcnow()
        game.live_phase = "in_progress"
        await self.db.commit()
        return {
            "game_id": game_id,
            "action": "start_second_half",
            "half2_started_at": str(game.half2_started_at),
        }

    async def reset_to_created(self, game_id: int) -> dict:
        """finished -> created. Clears all live/match data."""
        game = await self._lock_game(game_id)
        self._validate_transition(game, GameStatus.created, "reset_to_created")

        was_finished = game.status == GameStatus.finished

        game.status = GameStatus.created
        game.finished_at = None
        game.half1_started_at = None
        game.half2_started_at = None
        game.live_minute = None
        game.live_half = None
        game.live_phase = None
        game.home_score = None
        game.away_score = None
        game.home_penalty_score = None
        game.away_penalty_score = None
        await self.db.commit()

        if was_finished:
            self._enqueue_aggregate_repair(game)

        return {"game_id": game_id, "action": "reset_to_created"}

    async def set_postponed(self, game_id: int) -> dict:
        """created|live -> postponed. Terminal — no finished_at, no post-finish pipeline."""
        game = await self._lock_game(game_id)
        self._validate_transition(game, GameStatus.postponed, "set_postponed")
        game.status = GameStatus.postponed
        game.live_minute = None
        game.live_half = None
        game.live_phase = None
        await self.db.commit()
        return {"game_id": game_id, "action": "set_postponed"}

    async def set_cancelled(self, game_id: int) -> dict:
        """created|live -> cancelled."""
        game = await self._lock_game(game_id)
        self._validate_transition(game, GameStatus.cancelled, "set_cancelled")
        game.status = GameStatus.cancelled
        game.live_minute = None
        game.live_half = None
        game.live_phase = None
        await self.db.commit()
        return {"game_id": game_id, "action": "set_cancelled"}

    async def set_technical_defeat(self, game_id: int) -> dict:
        """live -> technical_defeat."""
        game = await self._lock_game(game_id)
        self._validate_transition(
            game, GameStatus.technical_defeat, "set_technical_defeat"
        )
        game.status = GameStatus.technical_defeat
        game.live_minute = None
        game.live_half = None
        game.live_phase = None
        await self.db.commit()
        return {"game_id": game_id, "action": "set_technical_defeat"}

    # ------------------------------------------------------------------ #
    #  Dispatch                                                           #
    # ------------------------------------------------------------------ #

    async def dispatch(self, game_id: int, action: str) -> dict:
        """Route an action string to the correct handler."""
        handlers = {
            "start_live": self.start_live,
            "finish_live": self.finish_live,
            "start_second_half": self.start_second_half,
            "reset_to_created": self.reset_to_created,
            "set_postponed": self.set_postponed,
            "set_cancelled": self.set_cancelled,
            "set_technical_defeat": self.set_technical_defeat,
        }
        handler = handlers.get(action)
        if not handler:
            raise ValueError(f"Unknown action: {action}")
        return await handler(game_id)

    # ------------------------------------------------------------------ #
    #  Helpers                                                            #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _enqueue_post_finish(game: Game) -> None:
        try:
            from app.tasks.live_tasks import post_finish_followup

            post_finish_followup.delay(game.id)
        except Exception:
            logger.exception(
                "Failed to enqueue post_finish_followup for game %s", game.id
            )

    @staticmethod
    def _enqueue_aggregate_repair(game: Game) -> None:
        """Log that season aggregates need manual resync after a reset.

        We intentionally do NOT auto-dispatch resync_extended_stats_task here
        because that task would re-sync stats from SOTA for a game that was
        just reset to created — the opposite of what we want.
        Use POST /ops/sync/team-season-stats and /ops/sync/player-season-stats
        to manually recompute after verifying the reset is correct.
        """
        logger.warning(
            "Game %s reset from finished -> created. Season aggregates for "
            "season_id=%s tour=%s may need manual resync via admin ops.",
            game.id,
            game.season_id,
            game.tour,
        )
