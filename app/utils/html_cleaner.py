"""HTML content cleaner and sanitizer for news articles."""
import re

import bleach
from bs4 import BeautifulSoup


def clean_news_content(html: str | None) -> str | None:
    """
    Clean news HTML content by removing wrapper elements.

    Removes: <style>, <script>, <main>, .hero-image, .hero-overlay, etc.
    Keeps: <p>, <h2-h6>, <ul>, <ol>, <table>, <blockquote>, <figure>, <iframe> (YouTube)
    """
    if not html:
        return html

    soup = BeautifulSoup(html, "lxml")

    # 0. Extract YouTube iframes before cleaning (they may be nested deep)
    youtube_iframes = []
    for iframe in soup.find_all('iframe'):
        src = iframe.get('src', '') or iframe.get('data-src', '')
        if 'youtube' in src:
            # Create a clean iframe tag
            clean_iframe = soup.new_tag('iframe')
            clean_iframe['src'] = src
            clean_iframe['width'] = '100%'
            clean_iframe['height'] = '500'
            clean_iframe['frameborder'] = '0'
            clean_iframe['allowfullscreen'] = 'allowfullscreen'
            clean_iframe['allow'] = 'accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture'
            youtube_iframes.append(str(clean_iframe))

    # 1. Remove unwanted elements
    for selector in [
        "style", "script",
        ".hero-image", ".hero-overlay", ".hero-content", ".hero-title",
        ".meta", ".news-date", ".premier-league",
        "nav", "header", "footer", ".navigation",
        "h1",  # Title already in separate field
    ]:
        for elem in soup.select(selector):
            elem.decompose()

    # 2. Try to find article body container
    article_body = soup.select_one(
        ".news-text, .article-body, .news-body, .content-text, "
        ".article-content, section.content"
    )

    working_elem = article_body if article_body else (soup.find("body") or soup)

    # 3. Collect text elements
    content_parts = []
    for child in working_elem.children:
        if hasattr(child, 'name'):
            if child.name in ['p', 'h2', 'h3', 'h4', 'h5', 'h6', 'ul', 'ol',
                              'table', 'blockquote', 'figure']:
                content_parts.append(child)
            elif child.name == 'div':
                text = child.get_text(strip=True)
                if text and len(text) > 20:
                    inner_tags = child.find_all(
                        ['p', 'h2', 'h3', 'h4', 'h5', 'h6', 'ul', 'ol', 'table', 'blockquote'],
                        recursive=False
                    )
                    if inner_tags:
                        content_parts.extend(inner_tags)
                    else:
                        content_parts.append(child)

    if not content_parts:
        content_parts = working_elem.find_all('p')

    # 4. Clean attributes and unwrap links
    result_parts = []
    for elem in content_parts:
        # Unwrap all <a> tags (keep text, remove link)
        for a_tag in elem.find_all('a'):
            a_tag.unwrap()

        # Remove all attributes except essential ones
        for tag in elem.find_all(True):
            tag.attrs = {k: v for k, v in tag.attrs.items()
                        if k in ['src', 'alt', 'title', 'colspan', 'rowspan']}
        elem.attrs = {k: v for k, v in elem.attrs.items()
                     if k in ['src', 'alt', 'title', 'colspan', 'rowspan']}
        result_parts.append(str(elem))

    # 5. Filter out date-only paragraphs and empty image paragraphs
    date_pattern = re.compile(r'^\d{2}\.\d{2}\.\d{4}$')
    filtered_parts = []
    for part in result_parts:
        # Parse to check content
        part_soup = BeautifulSoup(part, "lxml")
        text = part_soup.get_text(strip=True)

        # Skip date-only paragraphs
        if date_pattern.match(text):
            continue

        # Skip empty or image-only paragraphs
        if not text or text in ['', ' ']:
            continue

        filtered_parts.append(part)

    # 6. Clean whitespace
    result = "\n".join(filtered_parts)
    result = re.sub(r'<p>\s*</p>', '', result)
    result = re.sub(r'<p>\s*<strong>\s*<img[^>]*/>\s*</strong>\s*</p>', '', result)
    result = re.sub(r'<p>\s*<img[^>]*/>\s*</p>', '', result)
    result = re.sub(r'\n\s*\n', '\n', result)

    # 7. Append YouTube iframes at the end (wrapped in responsive div)
    if youtube_iframes:
        for iframe_html in youtube_iframes:
            result += f'\n<div class="video-container ratio ratio-16x9">{iframe_html}</div>'

    return result.strip() if result.strip() else None


# --- XSS sanitization (separate from clean_news_content above) ---
#
# sanitize_news_html() is the security boundary for HTML written into the
# News.content column. It is independent from clean_news_content() which is a
# heavy CMS-import normalizer (drops wrapper divs, unwraps <a>, throws away
# attributes). Running clean_news_content() on existing admin-saved content
# would destroy links and layout.
#
# Pipeline contract: admin save/update and the one-time backfill call ONLY
# sanitize_news_html(). clean_news_content() stays available for external CMS
# scrape paths; in that case sanitize_news_html() must run *after* it.

_ALLOWED_TAGS = frozenset({
    "p", "h2", "h3", "h4", "h5", "h6",
    "ul", "ol", "li",
    "table", "thead", "tbody", "tr", "td", "th",
    "blockquote", "figure", "figcaption",
    "img", "iframe",
    "strong", "em", "b", "i", "u",
    "a", "br", "hr", "span", "div",
})

_ALLOWED_ATTRS = {
    "a": ["href", "title", "target", "rel"],
    "img": ["src", "alt", "title", "width", "height"],
    "iframe": ["src", "allowfullscreen", "allow", "frameborder", "width", "height"],
    "td": ["colspan", "rowspan"],
    "th": ["colspan", "rowspan"],
}

_ALLOWED_PROTOCOLS = ["http", "https", "mailto"]

_YOUTUBE_EMBED_PREFIXES = (
    "https://www.youtube.com/embed/",
    "https://www.youtube-nocookie.com/embed/",
)

_bleach_cleaner = bleach.sanitizer.Cleaner(
    tags=_ALLOWED_TAGS,
    attributes=_ALLOWED_ATTRS,
    protocols=_ALLOWED_PROTOCOLS,
    strip=True,
    strip_comments=True,
)


_DROP_WITH_CONTENT_TAGS = ("script", "style", "noscript")


def sanitize_news_html(html: str | None) -> str | None:
    """Sanitize HTML for safe rendering via dangerouslySetInnerHTML.

    Strategy:
    1. BeautifulSoup pre-pass decomposes <script>/<style>/<noscript> entirely.
       Bleach's strip=True removes tags but preserves their text content, so
       <script>alert(1)</script> would leak "alert(1)" as visible text. Pre-pass
       fixes that.
    2. bleach.Cleaner enforces tag/attribute/protocol allowlist.
    3. BeautifulSoup post-pass:
       - <iframe> without a YouTube embed src is removed entirely.
       - <a> without href becomes plain text via unwrap().
       - <a target="_blank"> gets rel="noopener noreferrer" forced.

    Uses "html.parser" for fragment parsing (no <html>/<body>/<p> wrapping that
    "lxml" applies to bare text).
    """
    if not html:
        return html

    pre_soup = BeautifulSoup(html, "html.parser")
    for tag in pre_soup.find_all(_DROP_WITH_CONTENT_TAGS):
        tag.decompose()

    cleaned = _bleach_cleaner.clean(str(pre_soup))

    soup = BeautifulSoup(cleaned, "html.parser")

    for iframe in list(soup.find_all("iframe")):
        src = (iframe.get("src") or "").strip()
        if not src or not src.startswith(_YOUTUBE_EMBED_PREFIXES):
            iframe.decompose()

    for anchor in list(soup.find_all("a")):
        href = (anchor.get("href") or "").strip()
        if not href:
            anchor.unwrap()
            continue
        if anchor.get("target") == "_blank":
            anchor["rel"] = "noopener noreferrer"

    result = str(soup).strip()
    return result or None
