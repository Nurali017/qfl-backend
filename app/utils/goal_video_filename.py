"""Parser for goal video filenames uploaded to Google Drive.

Real-world examples (observed 2026-04-18):
  ``АБРАЕВ ГОЛ - 1 - Camera1 АБРАЕВ [18-06-24] [18-07-33].mp4``
  ``ГОЛ елимай - 1 - Camera1 ГОЛ ЕЛИМАЙ [17-28-20] [17-29-26].mp4``
  ``ГОЛ АСТАНА.mp4``
  ``жоржиньо гол - Camera1 жоржиньо [19-41-02] [19-41-32].mp4``
  ``СЕРГЕЙ МАЛЫЙ - 1 - Camera1 - [20-20-45] [20-22-06].mp4``
  ``Токтыбай - 1 - Camera1 - [20-49-43] [20-51-12].mp4``

Extraction rules:
  - **wall_time**: first ``[HH-MM-SS]`` pattern → camera timestamp of the clip
    (kickoff offset can be computed against ``games.half1_started_at``).
  - **player_hint**: first significant cyrillic/latin surname, skipping stop-words
    ("ГОЛ"/"CAMERA1"/team names can overlap with surnames — so we pick the *longest*
    candidate that is not a stopword and is not all-digits).
  - Bare numbers in the filename are usually camera indices (``- 1 -``),
    not minutes, so we no longer try to extract a minute from the filename.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ParsedGoal:
    wall_time: tuple[int, int, int] | None  # (H, M, S) — camera wall-clock time if present
    player_hint: str | None
    score_hint: str | None


_STOPWORDS = {
    # generic
    "тур", "round", "матч", "match", "гол", "goal",
    "video", "видео", "highlight", "клип", "clip",
    # camera / recording
    "camera", "cam", "кам", "дубль",
    "доп", "повтор", "replay",
    # team tags commonly appearing in names
    "кдл", "qfl", "fc", "fk", "мфк",
}

# Team names present in folder paths — don't let these hijack the player slot.
_TEAM_STOPWORDS = {
    "астана", "елимай", "кайрат", "каспий", "ертис", "жетысу",
    "ордабасы", "окжетпес", "иртыш", "тобыл", "улытау", "атырау",
    "кызылжар", "женис", "актобе", "кайсар", "алтай", "тараз",
    "шахтёр", "шахтер", "батыр", "туран", "арыс", "жайык",
    "хан-тенгри", "хан", "тенгри", "тобол", "ekibastuz", "astana",
    "kairat", "okzhetpes",
}

_WALLTIME_RE = re.compile(r"\[(\d{1,2})[-:](\d{1,2})[-:](\d{1,2})\]")
_SCORE_RE = re.compile(r"(?<!\d)(\d{1,2})\s*[-:]\s*(\d{1,2})(?!\d)")
_TOKEN_RE = re.compile(r"[A-Za-zА-Яа-яЁёҚқҒғҰұӘәІіҢңӨөҮүҺһ][A-Za-zА-Яа-яЁёҚқҒғҰұӘәІіҢңӨөҮүҺһ-]+")

_VIDEO_EXTENSIONS = {"mp4", "mov", "webm", "mkv", "m4v", "avi"}


def _strip_extension(name: str) -> str:
    idx = name.rfind(".")
    if idx == -1:
        return name
    ext = name[idx + 1:].lower()
    if ext in _VIDEO_EXTENSIONS:
        return name[:idx]
    return name


def _parse_wall_time(stem: str) -> tuple[int, int, int] | None:
    match = _WALLTIME_RE.search(stem)
    if not match:
        return None
    h, m, s = int(match.group(1)), int(match.group(2)), int(match.group(3))
    if not (0 <= h < 24 and 0 <= m < 60 and 0 <= s < 60):
        return None
    return (h, m, s)


def _pick_player(stem: str) -> str | None:
    # Remove bracketed sections (timestamps) to avoid noise.
    cleaned = re.sub(r"\[[^\]]*\]", " ", stem)
    candidates: list[str] = []
    for token in _TOKEN_RE.findall(cleaned):
        lower = token.lower()
        if lower in _STOPWORDS or lower in _TEAM_STOPWORDS:
            continue
        if len(token) < 4:
            continue
        candidates.append(token)
    if not candidates:
        return None
    # Prefer the LONGEST candidate (surnames tend to be longer than single
    # adjectives / camera labels). Ties → first occurrence.
    candidates.sort(key=lambda s: (-len(s), cleaned.find(s)))
    return candidates[0]


def parse_goal_filename(name: str) -> ParsedGoal | None:
    if not name:
        return None
    stem = _strip_extension(name).strip()
    if not stem:
        return None

    wall_time = _parse_wall_time(stem)
    player_hint = _pick_player(stem)

    score_match = _SCORE_RE.search(re.sub(r"\[[^\]]*\]", " ", stem))
    score_hint = f"{score_match.group(1)}-{score_match.group(2)}" if score_match else None

    if wall_time is None and player_hint is None:
        logger.debug("Cannot parse goal filename: %s", name)
        return None

    return ParsedGoal(wall_time=wall_time, player_hint=player_hint, score_hint=score_hint)
