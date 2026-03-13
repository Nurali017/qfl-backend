"""AI-powered match preview generator using Claude API."""

import logging
from datetime import datetime

import anthropic
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from sqlalchemy import or_, desc

from app.config import get_settings
from app.models import Game, GameStatus, Team, Stadium
from app.models.team_season_stats import TeamSeasonStats
from app.models.score_table import ScoreTable
from app.services.weather import format_weather

logger = logging.getLogger(__name__)
settings = get_settings()

PREVIEW_SYSTEM_PROMPT = """\
Ты — спортивный аналитик КФФ Лиги (Казахстан, футбол).
Напиши короткий тизер предстоящего матча (2-3 абзаца, 80-120 слов).

Стиль: живой спортивный язык, конкретные цифры из предоставленных данных, без клише и воды.
Главное: почему этот матч интересен и на что обратить внимание.
Если есть данные о расстановке — упомяни тактический рисунок команд.
Покрытие поля и погоду можно упомянуть как факт, но не акцентировать на них внимание.
НЕ давай прогноз на счёт. НЕ используй emoji.
Ответь ТОЛЬКО текстом превью, без заголовков и пояснений.
"""


class MatchPreviewGenerator:
    def __init__(self):
        if settings.anthropic_api_key:
            self._client = anthropic.AsyncAnthropic(
                api_key=settings.anthropic_api_key,
                max_retries=3,
                timeout=120,
            )
            self._model = settings.anthropic_model
        else:
            self._client = None

    @property
    def enabled(self) -> bool:
        return self._client is not None

    async def generate(self, game_id: int, lang: str, db: AsyncSession) -> str | None:
        """Generate a preview for a single game in the given language."""
        if not self._client:
            logger.warning("Anthropic API not configured, skipping preview generation")
            return None

        # Load game with relationships
        result = await db.execute(
            select(Game)
            .where(Game.id == game_id)
            .options(
                selectinload(Game.home_team),
                selectinload(Game.away_team),
                selectinload(Game.stadium_rel),
                selectinload(Game.season),
            )
        )
        game = result.scalar_one_or_none()
        if not game or not game.home_team or not game.away_team:
            return None

        # Collect context data
        context = await self._collect_context(game, db, lang)

        # Build prompt
        lang_label = "русском" if lang == "ru" else "казахском"
        user_prompt = f"Напиши превью на {lang_label} языке.\n\n{context}"

        try:
            response = await self._client.messages.create(
                model=self._model,
                max_tokens=500,
                system=PREVIEW_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_prompt}],
            )
            return response.content[0].text.strip()
        except Exception as e:
            logger.error(f"Failed to generate preview for game {game_id}: {e}")
            return None

    async def _collect_context(self, game: Game, db: AsyncSession, lang: str) -> str:
        """Collect all available context for the prompt."""
        lines = []

        home = game.home_team
        away = game.away_team

        # Basic match info
        lines.append("== МАТЧ ==")
        lines.append(f"{home.name} vs {away.name}, тур {game.tour}, {game.date}")

        # Stadium details
        stadium = game.stadium_rel
        if stadium:
            parts = [f"Стадион: {stadium.name}"]
            if stadium.city:
                parts.append(f"город: {stadium.city}")
            if stadium.capacity:
                parts.append(f"вместимость: {stadium.capacity}")
            if stadium.field_type:
                field_label = "искусственный газон" if stadium.field_type.value == "artificial" else "натуральный газон"
                parts.append(f"покрытие: {field_label}")
            lines.append(", ".join(parts))

        # Weather
        weather_str = format_weather(game.weather_temp, game.weather_condition, "ru")
        if weather_str:
            lines.append(f"Погода: {weather_str}")

        lines.append("")

        # Team stats for both teams
        for label, team, is_home in [("ХОЗЯЕВА", home, True), ("ГОСТИ", away, False)]:
            lines.append(f"== {label}: {team.name} ==")

            # Last formation from previous finished game
            formation = await self._get_last_formation(db, team.id, game.season_id, is_home)
            if formation:
                lines.append(f"Последняя расстановка: {formation}")

            # Season stats from TeamSeasonStats
            stats = await self._get_team_stats(db, team.id, game.season_id)
            if stats:
                lines.append(f"Сезон: {stats.win}П {stats.draw}Н {stats.match_loss}П, голы {stats.goal}:{stats.goals_conceded}")
                if stats.xg is not None:
                    lines.append(f"xG: {stats.xg}")
                if stats.possession_percent_average is not None:
                    lines.append(f"Владение: {stats.possession_percent_average}%")
                if stats.shot_per_match is not None:
                    lines.append(f"Удары/матч: {stats.shot_per_match}")
                if stats.pass_ratio is not None:
                    lines.append(f"Точность пасов: {stats.pass_ratio}%")
                if stats.duel_ratio is not None:
                    lines.append(f"Единоборства: {stats.duel_ratio}%")
                if stats.interception_per_match is not None:
                    lines.append(f"Перехваты/матч: {stats.interception_per_match}")

            # League standing
            standing = await self._get_standing(db, team.id, game.season_id)
            if standing:
                goal_diff = (standing.goals_scored or 0) - (standing.goals_conceded or 0)
                lines.append(f"Позиция: {standing.position}, {standing.points} очков, разница {goal_diff}")

            lines.append("")

        return "\n".join(lines)

    async def _get_team_stats(self, db: AsyncSession, team_id: int, season_id: int | None) -> TeamSeasonStats | None:
        if not season_id:
            return None
        result = await db.execute(
            select(TeamSeasonStats).where(
                TeamSeasonStats.team_id == team_id,
                TeamSeasonStats.season_id == season_id,
            )
        )
        return result.scalar_one_or_none()

    async def _get_last_formation(self, db: AsyncSession, team_id: int, season_id: int | None, is_home: bool) -> str | None:
        """Get team's formation from their last finished game in this season."""
        if not season_id:
            return None
        result = await db.execute(
            select(Game)
            .where(
                Game.season_id == season_id,
                Game.status.in_([GameStatus.finished, GameStatus.technical_defeat]),
                or_(Game.home_team_id == team_id, Game.away_team_id == team_id),
                Game.home_formation.isnot(None),
            )
            .order_by(desc(Game.date), desc(Game.time))
            .limit(1)
        )
        last_game = result.scalar_one_or_none()
        if not last_game:
            return None
        if last_game.home_team_id == team_id:
            return last_game.home_formation
        return last_game.away_formation

    async def _get_standing(self, db: AsyncSession, team_id: int, season_id: int | None):
        if not season_id:
            return None
        result = await db.execute(
            select(ScoreTable).where(
                ScoreTable.team_id == team_id,
                ScoreTable.season_id == season_id,
            )
        )
        return result.scalar_one_or_none()
