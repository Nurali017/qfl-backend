import pytest
import asyncio
from typing import AsyncGenerator, Generator
from uuid import uuid4
from datetime import date, time

from httpx import AsyncClient, ASGITransport
from sqlalchemy import event, String
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.pool import StaticPool
from sqlalchemy.dialects.postgresql import UUID as PGUUID

from app.main import app
from app.database import Base
from app.api.deps import get_db  # Import from where routes actually use it
from app.models import (
    Tournament, Season, Team, Player, PlayerTeam,
    Game, GameTeamStats, GamePlayerStats, ScoreTable,
    Page, News, Language
)


TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"


# Make PostgreSQL types work with SQLite
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.dialects.postgresql import UUID as PG_UUID, JSONB

@compiles(PG_UUID, "sqlite")
def compile_uuid_sqlite(type_, compiler, **kw):
    return "CHAR(36)"

@compiles(JSONB, "sqlite")
def compile_jsonb_sqlite(type_, compiler, **kw):
    return "JSON"


# Note: Using pytest-asyncio's built-in event_loop fixture (asyncio_mode = auto)
# Custom event_loop removed to avoid conflicts with pytest-asyncio 0.23.3+


@pytest.fixture(scope="function")
async def test_engine():
    """Create test database engine."""
    engine = create_async_engine(
        TEST_DATABASE_URL,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield engine

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

    await engine.dispose()


@pytest.fixture(scope="function")
async def test_session(test_engine) -> AsyncGenerator[AsyncSession, None]:
    """Create test database session."""
    async_session = async_sessionmaker(
        test_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )

    async with async_session() as session:
        yield session


@pytest.fixture(scope="function")
async def client(test_session) -> AsyncGenerator[AsyncClient, None]:
    """Create test client with overridden database dependency."""

    async def override_get_db():
        yield test_session

    app.dependency_overrides[get_db] = override_get_db

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test"
    ) as ac:
        yield ac

    app.dependency_overrides.clear()


# --- Data Fixtures ---

@pytest.fixture
async def sample_tournament(test_session) -> Tournament:
    """Create a sample tournament."""
    tournament = Tournament(id=7, name="Premier League")
    test_session.add(tournament)
    await test_session.commit()
    await test_session.refresh(tournament)
    return tournament


@pytest.fixture
async def sample_season(test_session, sample_tournament) -> Season:
    """Create a sample season."""
    season = Season(
        id=61,
        name="2025",
        tournament_id=sample_tournament.id,
        date_start=date(2025, 3, 1),
        date_end=date(2025, 11, 30),
    )
    test_session.add(season)
    await test_session.commit()
    await test_session.refresh(season)
    return season


@pytest.fixture
async def sample_teams(test_session) -> list[Team]:
    """Create sample teams."""
    teams = [
        Team(id=91, name="Astana", city="Astana"),
        Team(id=13, name="Kairat", city="Almaty"),
        Team(id=90, name="Tobol", city="Kostanay"),
    ]
    test_session.add_all(teams)
    await test_session.commit()
    for team in teams:
        await test_session.refresh(team)
    return teams


@pytest.fixture
async def sample_player(test_session) -> Player:
    """Create a sample player."""
    from app.models.player import Player
    player = Player(
        sota_id=uuid4(),
        first_name="Test",
        last_name="Player",
        birthday=date(1995, 1, 15),
        player_type="halfback",
        age=30,
        top_role="AM (attacking midfielder)",
    )
    test_session.add(player)
    await test_session.commit()
    await test_session.refresh(player)
    return player


@pytest.fixture
async def sample_game(test_session, sample_season, sample_teams) -> Game:
    """Create a sample game."""
    game = Game(
        sota_id=uuid4(),
        date=date(2025, 5, 15),
        time=time(18, 0),
        tour=1,
        season_id=sample_season.id,
        home_team_id=sample_teams[0].id,
        away_team_id=sample_teams[1].id,
        home_score=2,
        away_score=1,
        has_stats=True,
        stadium="Astana Arena",
        visitors=15000,
    )
    test_session.add(game)
    await test_session.commit()
    await test_session.refresh(game)
    return game


@pytest.fixture
async def sample_score_table(test_session, sample_season, sample_teams) -> list[ScoreTable]:
    """Create sample score table entries."""
    entries = []
    for i, team in enumerate(sample_teams):
        entry = ScoreTable(
            season_id=sample_season.id,
            team_id=team.id,
            position=i + 1,
            games_played=10,
            wins=7 - i,
            draws=2,
            losses=1 + i,
            goals_scored=20 - i * 3,
            goals_conceded=10 + i * 2,
            points=23 - i * 3,
        )
        entries.append(entry)
    test_session.add_all(entries)
    await test_session.commit()
    return entries


@pytest.fixture
async def sample_page(test_session) -> Page:
    """Create a sample page."""
    page = Page(
        slug="kontakty",
        language=Language.RU,
        title="Kontakty",
        content="<p>Contact information</p>",
        content_text="Contact information",
    )
    test_session.add(page)
    await test_session.commit()
    await test_session.refresh(page)
    return page


@pytest.fixture
async def sample_news(test_session) -> list[News]:
    """Create sample news articles."""
    from app.models.news import News
    news_items = [
        News(
            id=1,
            language=Language.RU,
            title="News Article 1",
            excerpt="Excerpt 1",
            content="<p>Content 1</p>",
            category="PREMIER-LIGA",
            tournament_id="pl",
            publish_date=date(2025, 5, 1),
        ),
        News(
            id=2,
            language=Language.RU,
            title="News Article 2",
            excerpt="Excerpt 2",
            content="<p>Content 2</p>",
            category="CUP",
            tournament_id="cup",
            publish_date=date(2025, 5, 2),
        ),
    ]
    test_session.add_all(news_items)
    await test_session.commit()
    return news_items
