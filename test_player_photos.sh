#!/bin/bash
# Тестовый скрипт для проверки фотографий игроков

echo "=========================================="
echo "ТЕСТ 1: Проверка MinIO напрямую"
echo "=========================================="
echo ""

# Прямой запрос к MinIO
curl -I "http://localhost:9000/qfl-files/player_photos/108a05e8-c690-4559-a6ef-d8c9b1992484.webp" 2>&1 | grep -E "HTTP|Content-Type|Content-Length"

echo ""
echo "=========================================="
echo "ТЕСТ 2: Скачать одно фото для проверки"
echo "=========================================="
echo ""

curl -o /tmp/test_player.webp "http://localhost:9000/qfl-files/player_photos/108a05e8-c690-4559-a6ef-d8c9b1992484.webp" 2>&1
if [ -f /tmp/test_player.webp ]; then
    echo "✓ Фото скачано успешно: /tmp/test_player.webp"
    ls -lh /tmp/test_player.webp
    file /tmp/test_player.webp
else
    echo "✗ Ошибка скачивания"
fi

echo ""
echo "=========================================="
echo "ТЕСТ 3: Проверка через API (если есть)"
echo "=========================================="
echo ""

# Проверка API endpoint для игрока
curl -s http://localhost:8000/api/players/108a05e8-c690-4559-a6ef-d8c9b1992484 2>&1 | python3 -m json.tool 2>/dev/null || echo "API endpoint не доступен или не настроен"

echo ""
echo "=========================================="
echo "ТЕСТ 4: Статистика по фото"
echo "=========================================="
echo ""

python3 -c "
import asyncio
import sys
sys.path.insert(0, '.')
from app.database import AsyncSessionLocal
from app.models.player import Player
from sqlalchemy import select, func

async def main():
    async with AsyncSessionLocal() as session:
        # Total players
        total = await session.execute(select(func.count(Player.id)))
        total_count = total.scalar()

        # Players with photos
        with_photos = await session.execute(
            select(func.count(Player.id)).where(Player.photo_url.isnot(None))
        )
        with_photos_count = with_photos.scalar()

        # Players with MinIO photos
        with_minio = await session.execute(
            select(func.count(Player.id)).where(Player.photo_url.like('%player_photos%'))
        )
        with_minio_count = with_minio.scalar()

        print(f'Всего игроков в БД:       {total_count}')
        print(f'С фото (любые):           {with_photos_count} ({with_photos_count*100//total_count}%)')
        print(f'С фото MinIO:             {with_minio_count} ({with_minio_count*100//total_count}%)')

asyncio.run(main())
" 2>&1

echo ""
echo "=========================================="
echo "ГОТОВО!"
echo "=========================================="
