"""
One-off script: insert Super Cup news (Kairat vs Tobyl, 28.02.2026).
Run from project root:  python -m scripts.add_supercup_news
"""

import asyncio
import uuid
from datetime import date

from sqlalchemy import select

from app.database import AsyncSessionLocal
from app.models.news import ArticleType, News
from app.models.page import Language


KZ_TITLE = "Қайрат – Тобыл: Қазақстан Суперкубогы үшін матч"
KZ_EXCERPT = (
    "28 ақпанда елордада жаңа футбол маусымының шымылдығы түріліп, "
    "алғашқы трофей сарапқа салынады."
)
KZ_CONTENT = """\
<p>28 ақпанда елордада жаңа футбол маусымының шымылдығы түріліп, алғашқы трофей сарапқа салынады.</p>

<p>Атап айтсақ, Қазақстан Суперкубогы үшін матчта ел чемпионы алматылық «Қайрат» пен Қазақстан Кубогының иегері қостанайлық «Тобыл» «Астана Арена» стадионында кездеседі. Амбициясы жоғары, мақсаты айқын қос команданың текетіресі футбол көктемінің шымылдығын ашып, мәртебелі жүлденің иесін анықтайды.</p>

<p>Ойын 28 ақпанда Астанада өтеді. Басталуы – 17:00.</p>

<p>Билеттер «Яндекс Афиша» сервисінде сатылымға шығады. Бағасы – 2000 теңгеден 8000 теңгеге дейін.</p>\
"""
KZ_CONTENT_TEXT = (
    "28 ақпанда елордада жаңа футбол маусымының шымылдығы түріліп, алғашқы трофей сарапқа салынады.\n\n"
    "Атап айтсақ, Қазақстан Суперкубогы үшін матчта ел чемпионы алматылық «Қайрат» пен "
    "Қазақстан Кубогының иегері қостанайлық «Тобыл» «Астана Арена» стадионында кездеседі. "
    "Амбициясы жоғары, мақсаты айқын қос команданың текетіресі футбол көктемінің шымылдығын ашып, "
    "мәртебелі жүлденің иесін анықтайды.\n\n"
    "Ойын 28 ақпанда Астанада өтеді. Басталуы – 17:00.\n\n"
    "Билеттер «Яндекс Афиша» сервисінде сатылымға шығады. Бағасы – 2000 теңгеден 8000 теңгеге дейін."
)

RU_TITLE = "Кайрат – Тобыл: матч за Суперкубок Казахстана"
RU_EXCERPT = (
    "28 февраля в столице Казахстана будет дан старт новому футбольному сезону "
    "– на кону первый трофей года."
)
RU_CONTENT = """\
<p>28 февраля в столице Казахстана будет дан старт новому футбольному сезону – на кону первый трофей года.</p>

<p>В матче за Суперкубок Казахстана на поле «Астана Арены» встретятся действующий чемпион страны – алматинский «Кайрат» – и обладатель Кубка Казахстана – костанайский «Тобыл». Принципиальное противостояние двух амбициозных коллективов откроет футбольную весну и определит обладателя почётного трофея.</p>

<p>Игра состоится в Астане 28 февраля, начало – в 17:00.</p>

<p>Билеты будут доступны на Яндекс Афише. Стоимость — от 2000 до 8000 тенге.</p>\
"""
RU_CONTENT_TEXT = (
    "28 февраля в столице Казахстана будет дан старт новому футбольному сезону – "
    "на кону первый трофей года.\n\n"
    "В матче за Суперкубок Казахстана на поле «Астана Арены» встретятся действующий чемпион "
    "страны – алматинский «Кайрат» – и обладатель Кубка Казахстана – костанайский «Тобыл». "
    "Принципиальное противостояние двух амбициозных коллективов откроет футбольную весну "
    "и определит обладателя почётного трофея.\n\n"
    "Игра состоится в Астане 28 февраля, начало – в 17:00.\n\n"
    "Билеты будут доступны на Яндекс Афише. Стоимость — от 2000 до 8000 тенге."
)


async def main() -> None:
    async with AsyncSessionLocal() as db:
        # Determine next available ID
        result = await db.execute(select(News.id).order_by(News.id.desc()).limit(1))
        current_max = result.scalar_one_or_none() or 0
        base_id = current_max + 1

        group_id = uuid.uuid4()
        publish = date(2026, 2, 26)

        ru = News(
            id=base_id,
            translation_group_id=group_id,
            language=Language.RU,
            title=RU_TITLE,
            excerpt=RU_EXCERPT,
            content=RU_CONTENT,
            content_text=RU_CONTENT_TEXT,
            championship_code="cup",
            article_type=ArticleType.NEWS,
            is_slider=True,
            slider_order=1,
            publish_date=publish,
        )
        kz = News(
            id=base_id + 1,
            translation_group_id=group_id,
            language=Language.KZ,
            title=KZ_TITLE,
            excerpt=KZ_EXCERPT,
            content=KZ_CONTENT,
            content_text=KZ_CONTENT_TEXT,
            championship_code="cup",
            article_type=ArticleType.NEWS,
            is_slider=True,
            slider_order=1,
            publish_date=publish,
        )

        db.add_all([ru, kz])
        await db.commit()

        print(f"Created news material (group_id={group_id})")
        print(f"  RU id={ru.id}  title={ru.title}")
        print(f"  KZ id={kz.id}  title={kz.title}")


if __name__ == "__main__":
    asyncio.run(main())
