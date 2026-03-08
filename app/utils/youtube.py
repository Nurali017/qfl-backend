"""YouTube URL parsing utilities."""

import re

_YOUTUBE_PATTERNS = [
    # youtube.com/watch?v=ID
    re.compile(r'(?:youtube\.com/watch\?.*?v=)([\w-]{11})'),
    # youtu.be/ID
    re.compile(r'youtu\.be/([\w-]{11})'),
    # youtube.com/embed/ID
    re.compile(r'youtube\.com/embed/([\w-]{11})'),
    # youtube.com/shorts/ID
    re.compile(r'youtube\.com/shorts/([\w-]{11})'),
]

_BARE_ID = re.compile(r'^[\w-]{11}$')


def extract_youtube_id(url: str) -> str | None:
    """Extract 11-char YouTube video ID from various URL formats or bare ID.

    Returns None if the input doesn't match any known pattern.
    """
    url = url.strip()
    if _BARE_ID.match(url):
        return url
    for pattern in _YOUTUBE_PATTERNS:
        m = pattern.search(url)
        if m:
            return m.group(1)
    return None
