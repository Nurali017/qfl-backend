import pytest

from app.models.news import ArticleType
from app.services.news_classifier import ClassificationDecision, NewsClassifierService


@pytest.mark.asyncio
async def test_classifier_rules_detect_analytics(monkeypatch):
    service = NewsClassifierService()
    monkeypatch.setattr(service, "_openai_enabled", False)
    monkeypatch.setattr(service, "client", None)

    decision = await service.classify_article(
        title="Тактический анализ матча",
        content_preview="Подробный разбор статистики и xG.",
        language="ru",
        min_confidence=0.7,
    )

    assert decision.article_type == ArticleType.ANALYTICS
    assert decision.confidence >= 0.7


@pytest.mark.asyncio
async def test_classifier_rules_detect_news(monkeypatch):
    service = NewsClassifierService()
    monkeypatch.setattr(service, "_openai_enabled", False)
    monkeypatch.setattr(service, "client", None)

    decision = await service.classify_article(
        title="Официально: клуб объявил о трансфере",
        content_preview="Официальный анонс и подписание контракта.",
        language="ru",
        min_confidence=0.7,
    )

    assert decision.article_type == ArticleType.NEWS
    assert decision.confidence >= 0.7


@pytest.mark.asyncio
async def test_classifier_ai_hit_when_rules_not_enough(monkeypatch):
    service = NewsClassifierService()

    monkeypatch.setattr(
        service,
        "_classify_with_rules",
        lambda title, preview: ClassificationDecision(
            article_type=None,
            confidence=0.2,
            source="rules",
            reason="no_rule_match",
        ),
    )

    async def fake_ai(title, preview, language):
        return ClassificationDecision(
            article_type=ArticleType.ANALYTICS,
            confidence=0.91,
            source="ai",
        )

    monkeypatch.setattr(service, "_classify_with_ai", fake_ai)

    decision = await service.classify_article(
        title="Материал без явных ключевых слов",
        content_preview="Текст",
        language="ru",
        min_confidence=0.7,
    )

    assert decision.article_type == ArticleType.ANALYTICS
    assert decision.source in {"ai", "rules+ai"}


@pytest.mark.asyncio
async def test_classifier_low_confidence_stays_unclassified(monkeypatch):
    service = NewsClassifierService()

    monkeypatch.setattr(
        service,
        "_classify_with_rules",
        lambda title, preview: ClassificationDecision(
            article_type=None,
            confidence=0.2,
            source="rules",
            reason="no_rule_match",
        ),
    )

    async def fake_ai(title, preview, language):
        return ClassificationDecision(
            article_type=ArticleType.NEWS,
            confidence=0.55,
            source="ai",
        )

    monkeypatch.setattr(service, "_classify_with_ai", fake_ai)

    decision = await service.classify_article(
        title="Нейтральный материал",
        content_preview="Очень коротко",
        language="ru",
        min_confidence=0.7,
    )

    assert decision.article_type is None
    assert decision.reason == "low_confidence"


@pytest.mark.asyncio
async def test_classifier_empty_content_returns_unclassified(monkeypatch):
    service = NewsClassifierService()
    monkeypatch.setattr(service, "_openai_enabled", False)
    monkeypatch.setattr(service, "client", None)

    decision = await service.classify_article(
        title="",
        content_preview="",
        language="ru",
        min_confidence=0.7,
    )

    assert decision.article_type is None
    assert decision.reason == "empty_content"
