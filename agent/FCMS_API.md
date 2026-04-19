# FCMS (FIFA CMS) — API Documentation

Дата исследования: 2026-03-20
URL: https://fcms.ma.services/kaz/home

---

## 1. Аутентификация

- **Login**: `POST https://auth-standard.fcms.ma.services/auth/signin`
- **Механизм**: httpOnly cookie `BEARER` на домене `.ma.services`
- **Токен**: 128-символьный hex (НЕ JWT), живёт ~30 мин
- **Доп. заголовок**: `x-customer-code: kaz`
- **Нет Authorization header** — cookie работает прозрачно
- **localStorage**: `authorized` (timestamp), `sessionDuration`, `language` (ru-RU)

---

## 2. Base URLs

| Сервис | URL |
|--------|-----|
| API | `https://api-standard.fcms.ma.services/v1/` |
| Auth | `https://auth-standard.fcms.ma.services/auth/` |
| Frontend | `https://fcms.ma.services/kaz/` |

- Customer code: `kaz` (КФФ)
- Customer ID: `30`

---

## 3. Формат ответов — HAL JSON

```json
{
  "page": 1,
  "limit": 10,
  "pages": 393,
  "total": 3922,
  "_links": { ... },
  "_embedded": {
    "matches": [ ... ]
  }
}
```

---

## 4. Эндпоинты

### Общие
| Method | Endpoint | Описание |
|--------|----------|----------|
| GET | `/v1/users/current` | Текущий пользователь |
| GET | `/v1/customers/current` | Текущий тенант (KFF) |
| HEAD | `/v1/customers/kaz/exist` | Проверка тенанта |
| GET | `/v1/featureToggles` | Фича-флаги |
| GET | `/v1/venues` | Стадионы |
| GET | `/v1/sports` | Виды спорта |

### Соревнования
| Method | Endpoint | Описание |
|--------|----------|----------|
| GET | `/v1/competitions?sort=id:desc` | Список соревнований |
| GET | `/v1/competitions/{id}` | Детали соревнования |
| GET | `/v1/competitions/{id}/groups` | Группы/этапы |

### Группы/Стадии
| Method | Endpoint | Описание |
|--------|----------|----------|
| GET | `/v1/groups/{id}` | Детали группы |
| GET | `/v1/groups/{id}/groupCompetitors` | Команды в группе |
| GET | `/v1/groups/{id}/standings` | Турнирная таблица |

### Матчи
| Method | Endpoint | Описание |
|--------|----------|----------|
| GET | `/v1/matches?sort=date:desc` | Список матчей |
| GET | `/v1/matches?filter[matchGroup][]=10733` | Матчи группы |
| GET | `/v1/matches/_dashboard?filter[date][]=gte:...&filter[date][]=lte:...&limit=900` | Календарь |
| GET | `/v1/matches/{id}` | Детали матча |
| GET | `/v1/matches/{id}/matchScores` | Счёт |
| GET | `/v1/matches/{id}/matchEvents` | События (голы, карточки, замены) |
| GET | `/v1/matches/{id}/matchDurationList` | Тайминг периодов |
| GET | `/v1/matches/{id}/matchOfficialAllocations` | Судьи |
| GET | `/v1/matches/{id}/competitors` | Участники (home/away) |
| GET | `/v1/matches/{id}/competitors/{compId}/matchPlayers` | Состав команды |
| GET | `/v1/matches/{id}/competitors/{compId}/matchTeamOfficials` | Тренерский штаб |
| GET | `/v1/matches/{id}/competitors/{compId}/matchSheetHistory` | История изменений листа |
| GET | `/v1/matches/{id}/matchDataHistory` | История данных матча |

### Конфликты расписания
| Method | Endpoint | Описание |
|--------|----------|----------|
| GET | `/v1/matches/teamConflicts?startDate=...&endDate=...` | Конфликты команд |
| GET | `/v1/matches/venueConflicts?startDate=...&endDate=...` | Конфликты стадионов |
| GET | `/v1/matches/matchOfficialAppointmentConflicts?role=REFEREE` | Конфликты судей |

### Клубы
| Method | Endpoint | Описание |
|--------|----------|----------|
| GET | `/v1/clubs?filter[type]=CLUB` | Список клубов |
| GET | `/v1/clubs?filter[type]=REPRESENTATIVE_CLUB` | Представители |
| GET | `/v1/clubs/{id}` | Детали клуба |
| GET | `/v1/clubs/{id}/teams?limit=50` | Команды клуба |
| GET | `/v1/clubs/{id}/persons` | Игроки и персонал |
| GET | `/v1/clubs/{id}/kitSets/{kitSetId}` | Форма клуба |

### Персоны
| Method | Endpoint | Описание |
|--------|----------|----------|
| GET | `/v1/persons?page=1` | Все персоны (игроки + офиц.) |
| GET | `/v1/teams/{id}` | Детали команды |

---

## 5. Структура объекта Match

```
id, number, matchDayNumber, round, status, date, time, timeZone,
groupId, groupTitle, venueId, venueTitle, venueInternationalTitle,
homeCompetitorId, homeCompetitorTitle, homeCompetitorInternationalTitle, homeCompetitorTeamId,
awayCompetitorId, awayCompetitorTitle, awayCompetitorInternationalTitle, awayCompetitorTeamId,
homeCompetitorScore, awayCompetitorScore, attendance,
homeCompetitorLogoLink, awayCompetitorLogoLink,
useLiveDataCollection, forfeited, winnerSide, isDraw,
matchSheetSubmissionDeadlineDatetime, pastMatchSheetSubmissionDeadline,
actualStartTime, actualEndTime, duration, temperature, weather,
editableFields, isMatchEventsSubmitted
```

### Статусы матча
- `SCHEDULED` — Запланированный
- `COMPLETED` — Завершено

---

## 6. Структура Venue

```json
{
  "id": 2976,
  "city": { "id": 4479976, "name": "Shymkent" },
  "title": "ДС Металлург",
  "internationalTitle": "Metallurg",
  "capacity": null,
  "sports": []
}
```

---

## 7. Навигация (UI)

### Главное меню
1. **Панель управления** — `/kaz/home`
2. **Клубы и люди**:
   - Клубы — `/kaz/clubs`
   - Представители — `/kaz/representatives`
   - Национальные сборные — `/kaz/nationalTeams`
   - Члены — `/kaz/members`
3. **Соревнования**:
   - Соревнования — `/kaz/competitions`
   - Места проведения — `/kaz/venues`
   - Матчи — `/kaz/matches`
   - Расписание — `/kaz/schedule-viewer`
   - Отчеты — `/kaz/reports`

### Вкладки матча
- **Листы матча** (Match Sheets) — составы, запасные, штаб, форма
- **Данные матча** (Match Data):
  - Общий — судьи, тайминг, посещаемость
  - События — голы, карточки, замены
  - PSO — серия пенальти
  - Заметки
- **Документы**
- **Официальные представители матча**
- **Дисциплинарные правила**

---

## 8. Известные ID

| Объект | ID | Название |
|--------|----|----------|
| Competition | 3517 | Премьер-Лига 2026 |
| Competition | 3585 | Первая Лига 2026 |
| Competition | 3596 | Вторая Лига 2026 (Юго-Запад) |
| Competition | 3597 | Вторая Лига 2026 (Северо-Восток) |
| Competition | 3598 | Кубок РК 2026 |
| Group | 10733 | Премьер-Лига основной этап |
| Group | 11083 | Вторая Лига ЮЗ основной этап |
| Group | 11084 | Вторая Лига СВ основной этап |
| Club | 11994 | Астана (FIFA ID: 109L60A) |
| Club | 12004 | Каспий |
| Club | 12005 | Оқжетпес |
| Team | 793 | Астана (Football, Male) |
| Match | 1009941 | Каспий 1:3 Оқжетпес |
| Customer | 30 | KAZ |

---

## 9. Что доступно / не доступно

### Доступно через FCMS API:
- Предматчевые составы (matchPlayers, matchTeamOfficials)
- Послематчевые протоколы (matchEvents, matchScores, matchDurationList)
- PDF отчёты (кнопка "Получить отчёт" в Match Data)
- Соревнования, группы, таблицы
- Клубы, игроки, стадионы

### НЕ доступно в FCMS (только FIFA Connect — kaz.ma.services):
- Заявки на сезон (регистрация команд)
- Трансферы / изменения

---

## 10. Аккаунт

- **Пользователь**: Tolepbergen Malikov
- **Роль**: Менеджер соревнований (Competition Manager)
- **Email**: lifetolep@gmail.com
- **Env**: `FCMS_BASE_URL`, `FCMS_EMAIL`, `FCMS_PASSWORD` в `backend/.env`
