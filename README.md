# QFL Backend - Футбольная статистика Казахстана

Backend приложение на FastAPI для интеграции с SOTA API (футбольная статистика Казахстана).

## Архитектура

```
SOTA API (sota.id) → Backend (FastAPI) → PostgreSQL → REST API → Frontend
```

## Технологии

- **FastAPI** - веб-фреймворк
- **SQLAlchemy 2.0** - ORM (асинхронный)
- **PostgreSQL** - база данных
- **Alembic** - миграции БД
- **Redis** - брокер для Celery
- **Celery** - фоновые задачи
- **httpx** - HTTP клиент для SOTA API
- **Docker** - контейнеризация

## Структура проекта

```
backend/
├── app/
│   ├── __init__.py
│   ├── main.py                 # FastAPI приложение
│   ├── config.py               # Настройки (env переменные)
│   ├── database.py             # SQLAlchemy подключение
│   │
│   ├── models/                 # SQLAlchemy модели
│   │   ├── tournament.py       # Турниры
│   │   ├── season.py           # Сезоны
│   │   ├── team.py             # Команды
│   │   ├── player.py           # Игроки
│   │   ├── player_team.py      # Связь игрок-команда-сезон
│   │   ├── game.py             # Матчи
│   │   ├── game_team_stats.py  # Статистика команды в матче
│   │   ├── game_player_stats.py # Статистика игрока в матче
│   │   └── score_table.py      # Турнирная таблица
│   │
│   ├── schemas/                # Pydantic схемы
│   │   ├── tournament.py
│   │   ├── season.py
│   │   ├── team.py
│   │   ├── player.py
│   │   ├── game.py
│   │   ├── stats.py
│   │   └── sync.py
│   │
│   ├── api/                    # API роуты
│   │   ├── router.py           # Главный роутер
│   │   ├── seasons.py          # /api/v1/seasons/*
│   │   ├── teams.py            # /api/v1/teams/*
│   │   ├── players.py          # /api/v1/players/*
│   │   ├── games.py            # /api/v1/games/*
│   │   └── sync.py             # /api/v1/sync/*
│   │
│   ├── services/               # Бизнес-логика
│   │   ├── sota_client.py      # Клиент для SOTA API
│   │   └── sync_service.py     # Синхронизация данных
│   │
│   └── tasks/                  # Celery задачи
│       ├── __init__.py         # Конфигурация Celery
│       └── sync_tasks.py       # Периодическая синхронизация
│
├── alembic/                    # Миграции БД
│   ├── env.py
│   └── versions/
│       └── 001_initial_tables.py
│
├── tests/
├── requirements.txt
├── docker-compose.yml
├── Dockerfile
├── .env.example
└── alembic.ini
```

## Быстрый старт

### Вариант 1: Docker (рекомендуется)

```bash
# Клонировать и перейти в директорию
cd backend

# Создать .env файл
cp .env.example .env

# Запустить все сервисы
docker-compose up --build

# В отдельном терминале - применить миграции
docker-compose exec backend alembic upgrade head

# Выполнить первую синхронизацию данных
curl -X POST http://localhost:8000/api/v1/sync/full
```

### Вариант 2: Локальная установка

```bash
# Создать виртуальное окружение
python -m venv venv
source venv/bin/activate  # Linux/Mac
# или
venv\Scripts\activate     # Windows

# Установить зависимости
pip install -r requirements.txt

# Создать .env файл
cp .env.example .env
# Отредактировать .env - указать свои данные PostgreSQL

# Применить миграции
alembic upgrade head

# Запустить сервер
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## Переменные окружения (.env)

```env
# База данных PostgreSQL
DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/qfl_db

# Учётные данные SOTA API
SOTA_API_EMAIL=kazachenko.galina@gmail.com
SOTA_API_PASSWORD=afd#@$afsad

# Redis (для Celery)
REDIS_URL=redis://localhost:6379/0

# Текущий сезон по умолчанию
CURRENT_SEASON_ID=61
```

## API Эндпоинты

Документация Swagger доступна по адресу: `http://localhost:8000/docs`

### Сезоны

| Метод | Эндпоинт | Описание |
|-------|----------|----------|
| GET | `/api/v1/seasons` | Список всех сезонов |
| GET | `/api/v1/seasons/{id}` | Детали сезона |
| GET | `/api/v1/seasons/{id}/table` | Турнирная таблица |
| GET | `/api/v1/seasons/{id}/games` | Матчи сезона |

### Команды

| Метод | Эндпоинт | Описание |
|-------|----------|----------|
| GET | `/api/v1/teams` | Список команд |
| GET | `/api/v1/teams/{id}` | Детали команды |
| GET | `/api/v1/teams/{id}/players` | Игроки команды |
| GET | `/api/v1/teams/{id}/games` | Матчи команды |
| GET | `/api/v1/teams/{id}/stats` | Статистика команды |

### Игроки

| Метод | Эндпоинт | Описание |
|-------|----------|----------|
| GET | `/api/v1/players` | Список игроков |
| GET | `/api/v1/players/{id}` | Детали игрока |
| GET | `/api/v1/players/{id}/stats` | Статистика игрока |
| GET | `/api/v1/players/{id}/games` | Матчи игрока |

### Матчи

| Метод | Эндпоинт | Описание |
|-------|----------|----------|
| GET | `/api/v1/games` | Список матчей |
| GET | `/api/v1/games/{id}` | Детали матча |
| GET | `/api/v1/games/{id}/stats` | Статистика матча |

### Синхронизация

| Метод | Эндпоинт | Описание |
|-------|----------|----------|
| POST | `/api/v1/sync/full` | Полная синхронизация |
| POST | `/api/v1/sync/games` | Синхронизация матчей |
| POST | `/api/v1/sync/teams` | Синхронизация команд |
| POST | `/api/v1/sync/players` | Синхронизация игроков |
| POST | `/api/v1/sync/score-table` | Синхронизация таблицы |
| POST | `/api/v1/sync/game-stats/{game_id}` | Статистика матча |

## Примеры запросов

### Получить турнирную таблицу

```bash
curl http://localhost:8000/api/v1/seasons/61/table
```

Ответ:
```json
{
  "season_id": 61,
  "table": [
    {
      "position": 1,
      "team_id": 91,
      "team_name": "Astana",
      "team_logo": "https://...",
      "games_played": 26,
      "wins": 18,
      "draws": 5,
      "losses": 3,
      "goals_scored": 52,
      "goals_conceded": 18,
      "goal_difference": 34,
      "points": 59,
      "form": "WWDWW"
    }
  ]
}
```

### Получить матчи команды

```bash
curl "http://localhost:8000/api/v1/teams/91/games?season_id=61"
```

### Полная синхронизация данных

```bash
curl -X POST "http://localhost:8000/api/v1/sync/full?season_id=61"
```

Ответ:
```json
{
  "status": "success",
  "message": "Full synchronization completed successfully",
  "details": {
    "tournaments": 5,
    "seasons": 12,
    "teams": 14,
    "players": 420,
    "games": 182,
    "score_table": 14,
    "game_stats_synced": 156
  }
}
```

## Celery задачи (фоновая синхронизация)

### Расписание автоматической синхронизации

| Задача | Расписание | Описание |
|--------|------------|----------|
| `sync_references` | Ежедневно в 6:00 | Турниры, сезоны, команды |
| `sync_games` | Каждые 2 часа | Матчи текущего сезона |
| `sync_live_stats` | Каждые 15 минут | Статистика недавних матчей |

### Запуск Celery вручную

```bash
# Запуск worker
celery -A app.tasks worker -l info

# Запуск scheduler
celery -A app.tasks beat -l info
```

## База данных

### Схема таблиц

```
tournaments          # Турниры (Premier League, First League...)
    └── seasons      # Сезоны (2024, 2025...)
        ├── games    # Матчи
        │   ├── game_team_stats    # Статистика команд
        │   └── game_player_stats  # Статистика игроков
        ├── player_teams           # Связь игрок-команда
        └── score_table            # Турнирная таблица

teams                # Команды (Astana, Kairat...)
players              # Игроки
```

### Миграции

```bash
# Создать новую миграцию
alembic revision --autogenerate -m "описание"

# Применить миграции
alembic upgrade head

# Откатить миграцию
alembic downgrade -1
```

## Docker сервисы

| Сервис | Порт | Описание |
|--------|------|----------|
| backend | 8000 | FastAPI приложение |
| db | 5432 | PostgreSQL база данных |
| redis | 6379 | Redis для Celery |
| celery_worker | - | Celery worker |
| celery_beat | - | Celery scheduler |

### Полезные команды Docker

```bash
# Запуск
docker-compose up -d

# Просмотр логов
docker-compose logs -f backend

# Перезапуск сервиса
docker-compose restart backend

# Остановка
docker-compose down

# Удаление с данными
docker-compose down -v
```

## SOTA API

### Особенности

- **Аутентификация**: JWT токены (access - 24ч, refresh - 12 дней)
- **Пагинация**: `count`, `next`, `previous`, `results`
- **ID форматы**:
  - UUID (string): `game_id`, `player_id`
  - Integer: `team_id`, `season_id`, `tournament_id`

### Публичные эндпоинты (без авторизации)

- `GET /api/public/v1/games/{game_id}/players/` - статистика игроков
- `GET /api/public/v1/games/{game_id}/teams/` - статистика команд

## Разработка

### Запуск в режиме разработки

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Проверка работоспособности

```bash
# Health check
curl http://localhost:8000/health

# Swagger UI
open http://localhost:8000/docs
```

## CMS Контент (Страницы и Новости)

### Импорт данных из JSON

Данные из папки `output/` можно импортировать в базу данных:

```bash
# Применить миграцию CMS
alembic upgrade head

# Запустить импорт
python scripts/import_cms_data.py
```

### Файлы для импорта

| Файл | Описание |
|------|----------|
| `contacts_kz.json`, `contacts_ru.json` | Страница контактов |
| `documents_kz.json`, `documents_ru.json` | Страница документов |
| `leadership_kz.json`, `leadership_ru.json` | Страница руководства |
| `news_kz.json`, `news_ru.json` | Новости (~9MB каждый) |

### CMS API эндпоинты

#### Страницы

| Метод | Эндпоинт | Описание |
|-------|----------|----------|
| GET | `/api/v1/pages?language=ru` | Список всех страниц |
| GET | `/api/v1/pages/{slug}?language=ru` | Страница по slug |
| GET | `/api/v1/pages/contacts/{language}` | Страница контактов |
| GET | `/api/v1/pages/documents/{language}` | Страница документов |
| GET | `/api/v1/pages/leadership/{language}` | Страница руководства |

#### Новости

| Метод | Эндпоинт | Описание |
|-------|----------|----------|
| GET | `/api/v1/news?language=ru&page=1&per_page=20` | Список новостей |
| GET | `/api/v1/news/{id}?language=ru` | Одна новость |
| GET | `/api/v1/news/latest?language=ru&limit=10` | Последние новости |
| GET | `/api/v1/news/categories?language=ru` | Список категорий |

### Примеры запросов

```bash
# Получить последние 10 новостей на казахском
curl "http://localhost:8000/api/v1/news/latest?language=kz&limit=10"

# Получить страницу руководства на русском
curl "http://localhost:8000/api/v1/pages/leadership/ru"

# Получить новости категории "ПРЕМЬЕР-ЛИГА"
curl "http://localhost:8000/api/v1/news?language=ru&category=ПРЕМЬЕР-ЛИГА"
```

## Лицензия

MIT
