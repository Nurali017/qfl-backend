"""AI-powered news classification service using OpenAI"""
from openai import AsyncOpenAI
from app.config import get_settings
from app.models.news import ArticleType, News
import asyncio
import logging
import re

logger = logging.getLogger(__name__)


class NewsClassifierService:
    """Classifies news articles using OpenAI GPT-4o-mini"""

    def __init__(self):
        settings = get_settings()
        self.client = AsyncOpenAI(
            api_key=settings.openai_api_key,
            max_retries=settings.openai_max_retries,
            timeout=settings.openai_timeout
        )
        self.model = settings.openai_model

    async def classify_article(
        self,
        title: str,
        content_preview: str,
        language: str = "ru"
    ) -> ArticleType:
        """
        Classify a single article.

        Args:
            title: Article title
            content_preview: First ~200 characters of article content
            language: Article language (kz or ru)

        Returns:
            ArticleType.NEWS or ArticleType.ANALYTICS
        """
        prompt = self._build_prompt(title, content_preview, language)

        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are a sports news classifier for a Kazakhstan football league website. "
                            "Classify articles into exactly two categories:\n\n"
                            "1. NEWS: Factual reporting about events, transfers, match results, "
                            "announcements, schedules, player/coach changes, organizational updates\n"
                            "2. ANALYTICS: Analysis, forecasts, predictions, tactical breakdowns, "
                            "statistical analysis, expert opinions, comparisons, performance reviews\n\n"
                            "Respond with ONLY one word: NEWS or ANALYTICS"
                        )
                    },
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=10
            )

            result = response.choices[0].message.content.strip().upper()

            if "ANALYTICS" in result:
                return ArticleType.ANALYTICS
            else:
                return ArticleType.NEWS

        except Exception as e:
            logger.error(f"Classification error: {e}")
            # Default to NEWS if classification fails
            return ArticleType.NEWS

    async def classify_batch(
        self,
        news_items: list[News],
        batch_size: int = 20
    ) -> dict[int, ArticleType]:
        """
        Classify multiple articles in batches.

        Args:
            news_items: List of News objects to classify
            batch_size: Number of concurrent API requests

        Returns:
            Dict mapping news.id to ArticleType
        """
        results = {}

        for i in range(0, len(news_items), batch_size):
            batch = news_items[i:i + batch_size]

            # Process batch concurrently
            tasks = [
                self._classify_news_item(news)
                for news in batch
            ]

            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            for news, result in zip(batch, batch_results):
                if isinstance(result, Exception):
                    logger.error(f"Error classifying news {news.id}: {result}")
                    results[news.id] = ArticleType.NEWS  # Default fallback
                else:
                    results[news.id] = result

            # Rate limiting: small delay between batches
            await asyncio.sleep(1)

        return results

    async def _classify_news_item(self, news: News) -> ArticleType:
        """Helper to classify a single News object"""
        content_preview = self._get_content_preview(news)
        language = news.language.value if news.language else "ru"

        return await self.classify_article(
            title=news.title,
            content_preview=content_preview,
            language=language
        )

    def _get_content_preview(self, news: News, max_chars: int = 200) -> str:
        """Extract content preview from news article"""
        # Prefer content_text (plain text), fallback to excerpt or content
        if news.content_text:
            return news.content_text[:max_chars]
        elif news.excerpt:
            return news.excerpt[:max_chars]
        elif news.content:
            # Strip HTML tags from content
            text = re.sub(r'<[^>]+>', '', news.content)
            return text[:max_chars]
        else:
            return ""

    def _build_prompt(self, title: str, content: str, language: str) -> str:
        """Build classification prompt"""
        return (
            f"Language: {language}\n"
            f"Title: {title}\n"
            f"Content preview: {content}\n\n"
            f"Classification:"
        )
