# API Документация для фронтенда (Публичная часть)

**Base URL:** `/api/v1`

---

## Мульти-турнирная поддержка

### Доступные турниры (season_id)

| season_id | Название (RU) | Название (KZ) | Название (EN) |
|-----------|---------------|---------------|---------------|
| 61 | Премьер-Лига | Премьер-Лига | Premier League |
| 62 | Первая Лига | Бірінші лига | First League |
| 63 | Кубок Казахстана | Қазақстан Кубогы | Kazakhstan Cup |
| 65 | Вторая Лига | Екінші лига | Second League |
| 66 | Женская Лига | Әйелдер лигасы | Women's League |

Для корректной работы с разными турнирами фронтенд **обязательно** должен передавать параметр `season_id` в следующих эндпоинтах:

| Эндпоинт | Параметр | По умолчанию |
|----------|----------|--------------|
| `GET /games` | `season_id` (query) | 61 (текущий сезон) |
| `GET /teams/{id}/players` | `season_id` (query) | 61 |
| `GET /teams/{id}/games` | `season_id` (query) | 61 |
| `GET /teams/{id}/stats` | `season_id` (query) | 61 |
| `GET /teams/{id}/coaches` | `season_id` (query) | 61 |
| `GET /teams/{id}/vs/{id}/head-to-head` | `season_id` (query) | 61 |
| `GET /players/{id}/stats` | `season_id` (query) | 61 |
| `GET /players/{id}/games` | `season_id` (query) | 61 |
| `GET /players/{id}/teammates` | `season_id` (query) | 61 |

**Важно:** Если параметр не передан, API использует `current_season_id` (61). Для корректного отображения данных по выбранному турниру (например, Кубок - season_id=63) фронтенд должен всегда передавать `season_id`.

**Примеры запросов:**
```
# Матчи Кубка
GET /api/v1/games?season_id=63&group_by_date=true

# Статистика команды в Первой лиге
GET /api/v1/teams/1/stats?season_id=62

# Таблица Женской лиги
GET /api/v1/seasons/66/table

# Игроки команды во Второй лиге
GET /api/v1/teams/5/players?season_id=65
```

### Синхронизация данных по турнирам

Для ручного запуска синхронизации данных по конкретному турниру используйте sync-эндпоинты с параметром `season_id`:

```
# Полная синхронизация для Кубка
POST /api/v1/sync/full?season_id=63

# Синхронизация матчей Первой лиги
POST /api/v1/sync/games?season_id=62

# Синхронизация таблицы Женской лиги
POST /api/v1/sync/score-table?season_id=66

# Синхронизация статистики команд Второй лиги
POST /api/v1/sync/team-season-stats?season_id=65

# Синхронизация статистики игроков Премьер-лиги
POST /api/v1/sync/player-season-stats?season_id=61
```

**Примечание:** По умолчанию (если `season_id` не указан) используется текущий сезон Премьер-лиги (61).

---

## 1. SEASONS (Сезоны)

| Метод | Endpoint | Описание |
|-------|----------|----------|
| GET | `/seasons` | Список всех сезонов |
| GET | `/seasons/{id}` | Сезон по ID |
| GET | `/seasons/{id}/table` | Турнирная таблица |
| GET | `/seasons/{id}/games?tour=5` | Матчи сезона (фильтр по туру) |

---

## 2. TEAMS (Команды)

| Метод | Endpoint | Описание |
|-------|----------|----------|
| GET | `/teams?season_id=2024` | Список команд |
| GET | `/teams/{id}` | Команда по ID |
| GET | `/teams/{id}/players?season_id=2024` | Игроки команды |
| GET | `/teams/{id}/games?season_id=2024` | Матчи команды |
| GET | `/teams/{id}/stats?season_id=2024` | Статистика команды |

---

## 3. PLAYERS (Игроки)

| Метод | Endpoint | Описание |
|-------|----------|----------|
| GET | `/players?season_id=2024&team_id=1&limit=50&offset=0` | Список игроков |
| GET | `/players/{uuid}` | Игрок по ID |
| GET | `/players/{uuid}/stats?season_id=2024` | Статистика игрока |
| GET | `/players/{uuid}/games?season_id=2024` | Матчи игрока |

---

## 4. GAMES (Матчи)

| Метод | Endpoint | Описание |
|-------|----------|----------|
| GET | `/games?season_id=2024&team_id=1&limit=50` | Список матчей |
| GET | `/games/{uuid}` | Матч по ID |
| GET | `/games/{uuid}/stats` | Статистика матча (команды + игроки) |

---

## 5. NEWS (Новости)

| Метод | Endpoint | Описание |
|-------|----------|----------|
| GET | `/news?language=ru&page=1&per_page=20` | Список новостей |
| GET | `/news/latest?language=ru&limit=10` | Последние новости |
| GET | `/news/categories?language=ru` | Категории |
| GET | `/news/{id}?language=ru` | Новость по ID |

**language:** `ru` или `kz`

---

## 6. PAGES (Страницы)

| Метод | Endpoint | Описание |
|-------|----------|----------|
| GET | `/pages?language=ru` | Все страницы |
| GET | `/pages/{slug}?language=ru` | Страница по slug |
| GET | `/pages/contacts/ru` | Контакты |
| GET | `/pages/documents/ru` | Документы |
| GET | `/pages/leadership/ru` | Руководство |

---

## Основные Response модели

### Team
```json
{ "id": 1, "name": "Aktobe", "logo_url": "https://..." }
```

### Player
```json
{
  "id": "uuid",
  "first_name": "Сергей",
  "last_name": "Петров",
  "photo_url": "https://...",
  "age": 25,
  "top_role": "FW",
  "country_name": "Казахстан"
}
```

### Game
```json
{
  "id": "uuid",
  "date": "2024-01-20",
  "time": "15:00:00",
  "tour": 5,
  "home_team": { "id": 1, "name": "Aktobe", "logo_url": "...", "score": 2 },
  "away_team": { "id": 2, "name": "Almaty", "logo_url": "...", "score": 1 },
  "stadium": "Aktobe Arena",
  "has_stats": true
}
```

### Турнирная таблица (table item)
```json
{
  "position": 1,
  "team_id": 1,
  "team_name": "Aktobe",
  "logo_url": "...",
  "games_played": 20,
  "wins": 12,
  "draws": 5,
  "losses": 3,
  "goals_scored": 35,
  "goals_conceded": 18,
  "goal_difference": 17,
  "points": 41,
  "form": "WWDLW"
}
```

### News
```json
{
  "id": 123,
  "title": "Заголовок",
  "excerpt": "Краткое описание...",
  "image_url": "https://...",
  "category": "Результаты",
  "publish_date": "2024-01-20"
}
```

---

## Пагинация

```
?limit=50&offset=0   // для players, games
?page=1&per_page=20  // для news
```

Response всегда содержит `total` - общее количество записей.

---

## Ошибки

```json
{ "detail": "Not found" }
```

- `200` - OK
- `404` - Не найдено
- `400` - Неверные параметры
