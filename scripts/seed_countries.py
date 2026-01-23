"""
Seed all countries with flags from external sources into local storage.

Usage:
    python scripts/seed_countries.py

This script:
1. Gets all countries from pycountry library
2. Downloads flags from flagcdn.com
3. Uploads flags to MinIO
4. Creates Country records in the database
"""

import asyncio
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx
import pycountry
from sqlalchemy import select

from app.database import AsyncSessionLocal
from app.models import Country
from app.services.file_storage import FileStorageService


# Country name translations (Russian and Kazakh)
COUNTRY_TRANSLATIONS = {
    "KZ": {"ru": "Казахстан", "kz": "Қазақстан"},
    "RU": {"ru": "Россия", "kz": "Ресей"},
    "UZ": {"ru": "Узбекистан", "kz": "Өзбекстан"},
    "KG": {"ru": "Киргизия", "kz": "Қырғызстан"},
    "TJ": {"ru": "Таджикистан", "kz": "Тәжікстан"},
    "TM": {"ru": "Туркменистан", "kz": "Түрікменстан"},
    "BY": {"ru": "Беларусь", "kz": "Беларусь"},
    "UA": {"ru": "Украина", "kz": "Украина"},
    "GE": {"ru": "Грузия", "kz": "Грузия"},
    "AM": {"ru": "Армения", "kz": "Армения"},
    "AZ": {"ru": "Азербайджан", "kz": "Әзербайжан"},
    "MD": {"ru": "Молдова", "kz": "Молдова"},
    "LT": {"ru": "Литва", "kz": "Литва"},
    "LV": {"ru": "Латвия", "kz": "Латвия"},
    "EE": {"ru": "Эстония", "kz": "Эстония"},
    "PL": {"ru": "Польша", "kz": "Польша"},
    "DE": {"ru": "Германия", "kz": "Германия"},
    "FR": {"ru": "Франция", "kz": "Франция"},
    "GB": {"ru": "Великобритания", "kz": "Ұлыбритания"},
    "US": {"ru": "США", "kz": "АҚШ"},
    "CN": {"ru": "Китай", "kz": "Қытай"},
    "JP": {"ru": "Япония", "kz": "Жапония"},
    "KR": {"ru": "Южная Корея", "kz": "Оңтүстік Корея"},
    "TR": {"ru": "Турция", "kz": "Түркия"},
    "IR": {"ru": "Иран", "kz": "Иран"},
    "BR": {"ru": "Бразилия", "kz": "Бразилия"},
    "AR": {"ru": "Аргентина", "kz": "Аргентина"},
    "ES": {"ru": "Испания", "kz": "Испания"},
    "IT": {"ru": "Италия", "kz": "Италия"},
    "PT": {"ru": "Португалия", "kz": "Португалия"},
    "NL": {"ru": "Нидерланды", "kz": "Нидерланды"},
    "BE": {"ru": "Бельгия", "kz": "Бельгия"},
    "SE": {"ru": "Швеция", "kz": "Швеция"},
    "NO": {"ru": "Норвегия", "kz": "Норвегия"},
    "FI": {"ru": "Финляндия", "kz": "Финляндия"},
    "DK": {"ru": "Дания", "kz": "Дания"},
    "AT": {"ru": "Австрия", "kz": "Австрия"},
    "CH": {"ru": "Швейцария", "kz": "Швейцария"},
    "CZ": {"ru": "Чехия", "kz": "Чехия"},
    "SK": {"ru": "Словакия", "kz": "Словакия"},
    "HU": {"ru": "Венгрия", "kz": "Венгрия"},
    "RO": {"ru": "Румыния", "kz": "Румыния"},
    "BG": {"ru": "Болгария", "kz": "Болгария"},
    "RS": {"ru": "Сербия", "kz": "Сербия"},
    "HR": {"ru": "Хорватия", "kz": "Хорватия"},
    "SI": {"ru": "Словения", "kz": "Словения"},
    "BA": {"ru": "Босния и Герцеговина", "kz": "Босния және Герцеговина"},
    "ME": {"ru": "Черногория", "kz": "Черногория"},
    "MK": {"ru": "Северная Македония", "kz": "Солтүстік Македония"},
    "AL": {"ru": "Албания", "kz": "Албания"},
    "GR": {"ru": "Греция", "kz": "Грекия"},
    "CY": {"ru": "Кипр", "kz": "Кипр"},
    "IL": {"ru": "Израиль", "kz": "Израиль"},
    "SA": {"ru": "Саудовская Аравия", "kz": "Сауд Арабиясы"},
    "AE": {"ru": "ОАЭ", "kz": "БАӘ"},
    "QA": {"ru": "Катар", "kz": "Қатар"},
    "EG": {"ru": "Египет", "kz": "Мысыр"},
    "MA": {"ru": "Марокко", "kz": "Марокко"},
    "NG": {"ru": "Нигерия", "kz": "Нигерия"},
    "ZA": {"ru": "ЮАР", "kz": "ОАР"},
    "AU": {"ru": "Австралия", "kz": "Австралия"},
    "NZ": {"ru": "Новая Зеландия", "kz": "Жаңа Зеландия"},
    "IN": {"ru": "Индия", "kz": "Үндістан"},
    "PK": {"ru": "Пакистан", "kz": "Пәкістан"},
    "AF": {"ru": "Афганистан", "kz": "Ауғанстан"},
    "MN": {"ru": "Монголия", "kz": "Моңғолия"},
    "TH": {"ru": "Таиланд", "kz": "Тайланд"},
    "VN": {"ru": "Вьетнам", "kz": "Вьетнам"},
    "ID": {"ru": "Индонезия", "kz": "Индонезия"},
    "MY": {"ru": "Малайзия", "kz": "Малайзия"},
    "SG": {"ru": "Сингапур", "kz": "Сингапур"},
    "PH": {"ru": "Филиппины", "kz": "Филиппин"},
    "MX": {"ru": "Мексика", "kz": "Мексика"},
    "CA": {"ru": "Канада", "kz": "Канада"},
    "CO": {"ru": "Колумбия", "kz": "Колумбия"},
    "CL": {"ru": "Чили", "kz": "Чили"},
    "PE": {"ru": "Перу", "kz": "Перу"},
    "VE": {"ru": "Венесуэла", "kz": "Венесуэла"},
    "CU": {"ru": "Куба", "kz": "Куба"},
    "IE": {"ru": "Ирландия", "kz": "Ирландия"},
    "IS": {"ru": "Исландия", "kz": "Исландия"},
}


async def download_flag(client: httpx.AsyncClient, code: str) -> bytes | None:
    """Download flag from flagcdn.com"""
    code_lower = code.lower()

    # Try different sources
    urls = [
        f"https://flagcdn.com/w320/{code_lower}.png",
        f"https://flagcdn.com/w160/{code_lower}.png",
        f"https://flagcdn.com/{code_lower}.svg",
    ]

    for url in urls:
        try:
            response = await client.get(url, timeout=10.0)
            if response.status_code == 200:
                return response.content
        except Exception:
            continue

    return None


async def seed_countries():
    """Seed all countries with flags."""
    print("Starting country seeding...")

    async with httpx.AsyncClient() as http_client:
        async with AsyncSessionLocal() as db:
            # Get existing countries
            result = await db.execute(select(Country.code))
            existing_codes = {row[0] for row in result.fetchall()}
            print(f"Found {len(existing_codes)} existing countries")

            created = 0
            updated = 0
            failed = 0

            # Get all countries from pycountry
            for country in pycountry.countries:
                code = country.alpha_2

                # Get translations or use English name
                translations = COUNTRY_TRANSLATIONS.get(code, {})
                name_ru = translations.get("ru", country.name)
                name_kz = translations.get("kz", country.name)
                name_en = country.name

                # Download flag
                print(f"Processing {code} - {name_en}...", end=" ")
                flag_data = await download_flag(http_client, code)

                flag_url = None
                if flag_data:
                    try:
                        upload_result = await FileStorageService.upload_country_flag(
                            file_data=flag_data,
                            country_code=code,
                            content_type="image/png",
                        )
                        flag_url = upload_result["url"]
                        print(f"flag OK", end=" ")
                    except Exception as e:
                        print(f"flag FAILED ({e})", end=" ")
                else:
                    print("no flag", end=" ")

                if code in existing_codes:
                    # Update existing
                    result = await db.execute(
                        select(Country).where(Country.code == code)
                    )
                    db_country = result.scalar_one()
                    db_country.name = name_ru
                    db_country.name_kz = name_kz
                    db_country.name_en = name_en
                    if flag_url:
                        db_country.flag_url = flag_url
                    updated += 1
                    print("updated")
                else:
                    # Create new
                    db_country = Country(
                        code=code,
                        name=name_ru,
                        name_kz=name_kz,
                        name_en=name_en,
                        flag_url=flag_url,
                    )
                    db.add(db_country)
                    created += 1
                    print("created")

            await db.commit()

            print(f"\nDone! Created: {created}, Updated: {updated}, Failed: {failed}")
            print(f"Total countries: {created + updated}")


if __name__ == "__main__":
    asyncio.run(seed_countries())
