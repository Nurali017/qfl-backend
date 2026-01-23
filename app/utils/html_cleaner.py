"""HTML content cleaner for news articles."""
import re
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
