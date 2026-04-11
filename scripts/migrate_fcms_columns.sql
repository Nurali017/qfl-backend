-- Migration: add FCMS mapping columns
-- Run on prod: psql -h localhost -U postgres -d qfl_db -f scripts/migrate_fcms_columns.sql

-- 1. Team mapping
ALTER TABLE teams ADD COLUMN IF NOT EXISTS fcms_team_id INTEGER UNIQUE;
CREATE INDEX IF NOT EXISTS ix_teams_fcms_team_id ON teams(fcms_team_id);

-- 2. Player mapping
ALTER TABLE players ADD COLUMN IF NOT EXISTS fcms_person_id INTEGER UNIQUE;
CREATE INDEX IF NOT EXISTS ix_players_fcms_person_id ON players(fcms_person_id);

-- 3. Populate team mapping (Премьер-Лига 2026, competition 3517)
UPDATE teams SET fcms_team_id = 793  WHERE id = 91;   -- Астана
UPDATE teams SET fcms_team_id = 794  WHERE id = 13;   -- Кайрат
UPDATE teams SET fcms_team_id = 795  WHERE id = 90;   -- Тобыл
UPDATE teams SET fcms_team_id = 2976 WHERE id = 51;   -- Актобе
UPDATE teams SET fcms_team_id = 2977 WHERE id = 49;   -- Атырау
UPDATE teams SET fcms_team_id = 2978 WHERE id = 47;   -- Каспий
UPDATE teams SET fcms_team_id = 2979 WHERE id = 87;   -- Кызылжар
UPDATE teams SET fcms_team_id = 2981 WHERE id = 81;   -- Ордабасы
UPDATE teams SET fcms_team_id = 3463 WHERE id = 92;   -- Женис
UPDATE teams SET fcms_team_id = 3464 WHERE id = 45;   -- Жетысу
UPDATE teams SET fcms_team_id = 3467 WHERE id = 94;   -- Кайсар
UPDATE teams SET fcms_team_id = 3469 WHERE id = 318;  -- Окжетпес
UPDATE teams SET fcms_team_id = 5751 WHERE id = 93;   -- Елимай
UPDATE teams SET fcms_team_id = 5752 WHERE id = 293;  -- Улытау
UPDATE teams SET fcms_team_id = 8330 WHERE id = 595;  -- Иртыш
UPDATE teams SET fcms_team_id = 8331 WHERE id = 295;  -- Алтай

-- 4. Populate team mapping (Первая Лига 2026, competition 3585)
UPDATE teams SET fcms_team_id = 796  WHERE id = 46;    -- Шахтёр
UPDATE teams SET fcms_team_id = 2982 WHERE id = 646;   -- Тараз
UPDATE teams SET fcms_team_id = 2983 WHERE id = 80;    -- Туран
UPDATE teams SET fcms_team_id = 3460 WHERE id = 649;   -- Академия Оңтүстік
UPDATE teams SET fcms_team_id = 3461 WHERE id = 626;   -- Астана М
UPDATE teams SET fcms_team_id = 3466 WHERE id = 14;    -- Кайрат-Жастар
UPDATE teams SET fcms_team_id = 3472 WHERE id = 294;   -- Экибастуз
UPDATE teams SET fcms_team_id = 3489 WHERE id = 292;   -- Актобе-М
UPDATE teams SET fcms_team_id = 3482 WHERE id = 621;   -- Тобол М
UPDATE teams SET fcms_team_id = 3483 WHERE id = 644;   -- Хан-Тенгри
UPDATE teams SET fcms_team_id = 3490 WHERE id = 333;   -- Арыс
UPDATE teams SET fcms_team_id = 5822 WHERE id = 623;   -- Елимай М
UPDATE teams SET fcms_team_id = 11292 WHERE id = 633;  -- Жайык
UPDATE teams SET fcms_team_id = 3493 WHERE id = 624;   -- Каспий М
