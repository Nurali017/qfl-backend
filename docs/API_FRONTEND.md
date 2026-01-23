# API Документация для фронтенда (Публичная часть)

**Base URL:** `/api/v1`

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
