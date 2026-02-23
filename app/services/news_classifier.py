"""Rule-based + AI-powered service for classifying news as NEWS/ANALYTICS."""

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass
from typing import Sequence

from openai import AsyncOpenAI

from app.config import get_settings
from app.models.news import ArticleType, News

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ClassificationDecision:
    article_type: ArticleType | None
    confidence: float
    source: str
    reason: str | None = None
    representative_news_id: int | None = None
    representative_title: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "article_type": self.article_type.value if self.article_type else None,
            "confidence": round(self.confidence, 4),
            "source": self.source,
            "reason": self.reason,
            "representative_news_id": self.representative_news_id,
            "representative_title": self.representative_title,
        }


class NewsClassifierService:
    """Classifies news articles into NEWS/ANALYTICS with confidence gating."""

    _RULE_SHORT_CIRCUIT_CONFIDENCE = 0.85
    _DEFAULT_CONFIDENCE = 0.70

    _ANALYTICS_PATTERNS = (
        r"\bанализ\w*",
        r"\bразбор\w*",
        r"\bпрогноз\w*",
        r"\bмнен\w*",
        r"\bтактик\w*",
        r"\bстатистик\w*",
        r"\bобзор\w*",
        r"\bxg\b",
        r"\bexpected goals?\b",
        r"\bpreview\b",
        r"\banalys\w*\b",
        r"\bperformance review\b",
        r"\bталдау\w*",
        r"\bболжам\w*",
        r"\bсарап\w*",
    )
    _NEWS_PATTERNS = (
        r"\bофициальн\w*",
        r"\bобъяв\w*",
        r"\bназнач\w*",
        r"\bпереш\w*",
        r"\bтрансфер\w*",
        r"\bподпис\w*",
        r"\bрезультат\w*",
        r"\bрасписан\w*",
        r"\bинтервью\b",
        r"\bofficial\b",
        r"\bannouncement\b",
        r"\btransfer\b",
        r"\bfixture\b",
        r"\bmatchday\b",
        r"\bжаңалық\w*",
        r"\bресми\w*",
    )

    def __init__(self):
        settings = get_settings()
        self.model = settings.openai_model
        self._openai_enabled = bool(settings.openai_api_key)
        self.client: AsyncOpenAI | None = None
        if self._openai_enabled:
            self.client = AsyncOpenAI(
                api_key=settings.openai_api_key,
                max_retries=settings.openai_max_retries,
                timeout=settings.openai_timeout,
            )

        self._analytics_regex = [re.compile(pattern, flags=re.IGNORECASE) for pattern in self._ANALYTICS_PATTERNS]
        self._news_regex = [re.compile(pattern, flags=re.IGNORECASE) for pattern in self._NEWS_PATTERNS]

    async def classify_article(
        self,
        title: str,
        content_preview: str,
        language: str = "ru",
        min_confidence: float = _DEFAULT_CONFIDENCE,
    ) -> ClassificationDecision:
        """
        Classify a single article using rules first, then AI if needed.

        Result may be unclassified (article_type=None) when confidence is low.
        """
        title = (title or "").strip()
        content_preview = (content_preview or "").strip()
        if not title and not content_preview:
            return ClassificationDecision(
                article_type=None,
                confidence=0.0,
                source="rules",
                reason="empty_content",
            )

        rules_decision = self._classify_with_rules(title, content_preview)
        if (
            rules_decision.article_type is not None
            and rules_decision.confidence >= self._RULE_SHORT_CIRCUIT_CONFIDENCE
        ):
            final_decision = rules_decision
        else:
            ai_decision = await self._classify_with_ai(title, content_preview, language)
            final_decision = self._merge_decisions(rules_decision, ai_decision)

        if final_decision.article_type is None:
            return final_decision
        if final_decision.confidence < min_confidence:
            return ClassificationDecision(
                article_type=None,
                confidence=final_decision.confidence,
                source=final_decision.source,
                reason="low_confidence",
            )
        return final_decision

    async def classify_group(
        self,
        news_items: Sequence[News],
        min_confidence: float = _DEFAULT_CONFIDENCE,
    ) -> ClassificationDecision:
        """
        Classify one translation group with language preference RU -> KZ -> longest text.
        """
        representative = self._pick_representative_news(news_items)
        if representative is None:
            return ClassificationDecision(
                article_type=None,
                confidence=0.0,
                source="rules",
                reason="empty_group",
            )

        content_preview = self._get_content_preview(representative, max_chars=1200)
        language = representative.language.value if representative.language else "ru"
        decision = await self.classify_article(
            title=representative.title or "",
            content_preview=content_preview,
            language=language,
            min_confidence=min_confidence,
        )
        decision.representative_news_id = representative.id
        decision.representative_title = representative.title
        return decision

    async def classify_batch(
        self,
        news_items: list[News],
        batch_size: int = 20,
        min_confidence: float = _DEFAULT_CONFIDENCE,
    ) -> dict[int, ArticleType | None]:
        """Compatibility helper for item-level batch classification."""
        results: dict[int, ArticleType | None] = {}
        for i in range(0, len(news_items), batch_size):
            batch = news_items[i : i + batch_size]
            tasks = [self._classify_news_item(news, min_confidence=min_confidence) for news in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            for news, result in zip(batch, batch_results):
                if isinstance(result, Exception):
                    logger.exception("Error classifying news %s", news.id)
                    results[news.id] = None
                else:
                    results[news.id] = result

            await asyncio.sleep(0.1)

        return results

    async def _classify_news_item(
        self,
        news: News,
        min_confidence: float = _DEFAULT_CONFIDENCE,
    ) -> ArticleType | None:
        content_preview = self._get_content_preview(news)
        language = news.language.value if news.language else "ru"
        decision = await self.classify_article(
            title=news.title or "",
            content_preview=content_preview,
            language=language,
            min_confidence=min_confidence,
        )
        return decision.article_type

    def _classify_with_rules(self, title: str, content_preview: str) -> ClassificationDecision:
        title_text = self._normalize_text(title)
        content_text = self._normalize_text(content_preview)
        combined = f"{title_text}\n{content_text}".strip()
        if not combined:
            return ClassificationDecision(None, 0.0, "rules", "empty_content")

        analytics_score = self._score_text(title_text, content_text, self._analytics_regex)
        news_score = self._score_text(title_text, content_text, self._news_regex)
        if analytics_score == 0 and news_score == 0:
            return ClassificationDecision(None, 0.0, "rules", "no_rule_match")

        diff = abs(analytics_score - news_score)
        total = analytics_score + news_score
        confidence = min(0.95, 0.55 + (diff / max(total, 1)) * 0.35 + min(total, 5) * 0.02)

        if diff <= 1:
            return ClassificationDecision(None, min(confidence, 0.69), "rules", "ambiguous_rule_match")

        article_type = (
            ArticleType.ANALYTICS if analytics_score > news_score else ArticleType.NEWS
        )
        return ClassificationDecision(article_type, confidence, "rules")

    async def _classify_with_ai(
        self,
        title: str,
        content_preview: str,
        language: str,
    ) -> ClassificationDecision:
        if not self.client or not self._openai_enabled:
            return ClassificationDecision(None, 0.0, "ai", "openai_disabled")

        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You classify football website articles into NEWS or ANALYTICS. "
                            "NEWS: factual event reporting. ANALYTICS: tactical/statistical/opinion analysis. "
                            "Return a strict JSON object: "
                            "{\"type\":\"NEWS|ANALYTICS|UNCLASSIFIED\",\"confidence\":0..1,\"reason\":\"short reason\"}."
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"Language: {language}\n"
                            f"Title: {title}\n"
                            f"Content preview: {content_preview}"
                        ),
                    },
                ],
                temperature=0.0,
                max_tokens=120,
            )
            content = (response.choices[0].message.content or "").strip()
            parsed = self._parse_ai_payload(content)
            if parsed["type"] == "UNCLASSIFIED":
                return ClassificationDecision(
                    article_type=None,
                    confidence=parsed["confidence"],
                    source="ai",
                    reason=parsed["reason"] or "ai_unclassified",
                )
            article_type = ArticleType(parsed["type"])
            return ClassificationDecision(
                article_type=article_type,
                confidence=parsed["confidence"],
                source="ai",
                reason=parsed["reason"],
            )
        except Exception:
            logger.exception("AI classification error")
            return ClassificationDecision(None, 0.0, "ai", "ai_error")

    @staticmethod
    def _parse_ai_payload(payload: str) -> dict[str, str | float]:
        try:
            raw = json.loads(payload)
            raw_type = str(raw.get("type", "UNCLASSIFIED")).upper()
            raw_confidence = float(raw.get("confidence", 0.65))
            raw_reason = str(raw.get("reason", "")).strip() or None
        except Exception:
            normalized = payload.strip().upper()
            if "ANALYTICS" in normalized:
                return {"type": "ANALYTICS", "confidence": 0.65, "reason": "ai_text_fallback"}
            if "NEWS" in normalized:
                return {"type": "NEWS", "confidence": 0.65, "reason": "ai_text_fallback"}
            return {"type": "UNCLASSIFIED", "confidence": 0.0, "reason": "ai_parse_failed"}

        if raw_type not in {"NEWS", "ANALYTICS", "UNCLASSIFIED"}:
            raw_type = "UNCLASSIFIED"
        confidence = min(max(raw_confidence, 0.0), 1.0)
        return {"type": raw_type, "confidence": confidence, "reason": raw_reason}

    @staticmethod
    def _merge_decisions(
        rules_decision: ClassificationDecision,
        ai_decision: ClassificationDecision,
    ) -> ClassificationDecision:
        if rules_decision.article_type and ai_decision.article_type:
            if rules_decision.article_type == ai_decision.article_type:
                return ClassificationDecision(
                    article_type=rules_decision.article_type,
                    confidence=max(rules_decision.confidence, ai_decision.confidence),
                    source="rules+ai",
                )
            if abs(rules_decision.confidence - ai_decision.confidence) < 0.15:
                return ClassificationDecision(
                    article_type=None,
                    confidence=max(rules_decision.confidence, ai_decision.confidence),
                    source="rules+ai",
                    reason="conflicting_signals",
                )
            return (
                rules_decision
                if rules_decision.confidence > ai_decision.confidence
                else ai_decision
            )
        if ai_decision.article_type:
            return ai_decision
        if rules_decision.article_type:
            return rules_decision
        if ai_decision.reason:
            return ai_decision
        return rules_decision

    @staticmethod
    def _normalize_text(text: str | None) -> str:
        value = (text or "").strip()
        value = re.sub(r"<[^>]+>", " ", value)
        value = re.sub(r"\s+", " ", value)
        return value

    @staticmethod
    def _score_text(
        title_text: str,
        content_text: str,
        patterns: Sequence[re.Pattern[str]],
    ) -> int:
        score = 0
        for pattern in patterns:
            score += len(pattern.findall(title_text)) * 2
            score += len(pattern.findall(content_text))
        return score

    def _pick_representative_news(self, news_items: Sequence[News]) -> News | None:
        if not news_items:
            return None

        for preferred_lang in ("ru", "kz"):
            for item in news_items:
                if item.language and item.language.value == preferred_lang and self._has_meaningful_text(item):
                    return item

        meaningful = [item for item in news_items if self._has_meaningful_text(item)]
        if not meaningful:
            return news_items[0]

        return max(
            meaningful,
            key=lambda item: len((item.title or "") + self._get_content_preview(item, max_chars=1200)),
        )

    def _has_meaningful_text(self, news: News) -> bool:
        return bool((news.title or "").strip() or self._get_content_preview(news))

    def _get_content_preview(self, news: News, max_chars: int = 600) -> str:
        if news.content_text:
            text = news.content_text
        elif news.excerpt:
            text = news.excerpt
        elif news.content:
            text = re.sub(r"<[^>]+>", " ", news.content)
        else:
            text = ""
        text = re.sub(r"\s+", " ", text).strip()
        return text[:max_chars]
