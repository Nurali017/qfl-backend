"""Parse pre-match report PDFs to extract lineups for both teams."""

import re
import logging
from dataclasses import dataclass, field

import fitz  # PyMuPDF

logger = logging.getLogger(__name__)

# Section markers
_STARTERS_RE = re.compile(r"СТАРТОВЫЙ", re.IGNORECASE)
_SUBS_RE = re.compile(r"ЗАМЕН", re.IGNORECASE)  # catches ЗАМЕНЬ, ЗАМЕНЫ

# Player block: "74\nМухаммеджан Сейсен" (number on first line, name on second)
_NUMBER_RE = re.compile(r"^(\d+)$")
_PLAYER_BLOCK_RE = re.compile(r"^(\d+)\n(.+)$", re.MULTILINE)


@dataclass
class ParsedPlayer:
    shirt_number: int
    first_name: str
    last_name: str
    is_goalkeeper: bool = False
    is_captain: bool = False


@dataclass
class ParsedTeamLineup:
    team_name: str = ""
    starters: list[ParsedPlayer] = field(default_factory=list)
    substitutes: list[ParsedPlayer] = field(default_factory=list)


@dataclass
class PrematchParseResult:
    home: ParsedTeamLineup = field(default_factory=ParsedTeamLineup)
    away: ParsedTeamLineup = field(default_factory=ParsedTeamLineup)
    match_number: int | None = None


def parse_prematch_pdf(pdf_bytes: bytes) -> PrematchParseResult:
    """Parse a pre-match report PDF and extract lineups for both teams.

    The PDF has a two-column layout:
    - Home team data at x0 ~50-200
    - Away team data at x0 ~250-440
    - Officials sidebar at x0 ~450+
    - Player blocks contain "number\\nname" (newline within block)

    GK/Captain markers are vector graphics (not text), so:
    - The first player in each starters list is marked as goalkeeper
    """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")

    home_blocks: list[tuple[float, str]] = []
    away_blocks: list[tuple[float, str]] = []
    all_blocks: list[tuple[float, float, str]] = []  # (x0, y0, text) for metadata

    for page in doc:
        width = page.rect.width
        # The PDF has 3 zones: home (~0-200), away (~200-450), officials (~450+)
        # Use ~40% of width as split between home and away columns
        col_split = width * 0.38  # ~226 for 595-wide page

        for block in page.get_text("blocks"):
            x0, y0, x1, _y1, text, _block_no, block_type = block
            if block_type != 0:  # skip image blocks
                continue
            text = text.strip()
            if not text:
                continue

            all_blocks.append((x0, y0, text))

            # Skip officials sidebar (right 25% of page)
            if x0 > width * 0.75:
                continue

            if x0 < col_split:
                home_blocks.append((y0, text))
            else:
                away_blocks.append((y0, text))

    doc.close()

    # Sort by vertical position
    home_blocks.sort(key=lambda b: b[0])
    away_blocks.sort(key=lambda b: b[0])

    # Extract match number from all blocks
    match_number = _extract_match_number(all_blocks)

    # Parse each column
    home = _parse_column_blocks(home_blocks)
    away = _parse_column_blocks(away_blocks)

    # Mark first starter as goalkeeper (standard convention, markers are graphics)
    if home.starters:
        home.starters[0].is_goalkeeper = True
    if away.starters:
        away.starters[0].is_goalkeeper = True

    return PrematchParseResult(
        home=home,
        away=away,
        match_number=match_number,
    )


def _extract_match_number(blocks: list[tuple[float, float, str]]) -> int | None:
    """Extract match number from blocks, looking for 'Матч №: 8'."""
    for _x, _y, text in blocks:
        m = re.search(r"Матч\s*№\s*:?\s*(\d+)", text)
        if m:
            return int(m.group(1))
    return None


def _parse_column_blocks(blocks: list[tuple[float, str]]) -> ParsedTeamLineup:
    """Parse blocks from one column (home or away) into a team lineup.

    Blocks contain text like "74\\nМухаммеджан Сейсен" (number and name
    separated by newline within the same block).
    """
    result = ParsedTeamLineup()
    section: str | None = None  # "starters" or "substitutes"
    team_name_candidates: list[str] = []

    for _y, text in blocks:
        # Check each line in the block for section markers
        lines = text.split("\n")

        # Section detection on the full block text
        if _STARTERS_RE.search(text):
            section = "starters"
            continue
        if _SUBS_RE.search(text):
            section = "substitutes"
            continue

        # Skip table headers
        if text.strip() in ("#\nИгрок", "# Игрок", "#", "Игрок"):
            continue

        # Before any section — collect team name candidates
        if section is None:
            first_line = lines[0].strip()
            if first_line and not re.match(r"^\d+$", first_line):
                # Filter out generic labels and metadata
                upper = first_line.upper()
                skip_keywords = (
                    "ДОМАШНЯЯ", "КОМАНДА", "ГОСТ", "ЧЕМПИОНАТ",
                    "ЛИГИ", "РАУНД", "ДЕНЬ", "―", "ОТЧЕТ",
                    "ПРЕМЬЕР", "МАТЧА", "UTC", "ASIA/", "/20",
                )
                if not any(kw in upper for kw in skip_keywords):
                    team_name_candidates.append(first_line)
            continue

        # Try to parse player from block (format: "number\nname")
        player = _parse_player_block(text)
        if player:
            if section == "starters":
                result.starters.append(player)
            elif section == "substitutes":
                result.substitutes.append(player)

    # Use the first team name candidate that looks like a team name
    for candidate in team_name_candidates:
        if len(candidate) >= 2:
            result.team_name = candidate
            break

    return result


def _parse_player_block(text: str) -> ParsedPlayer | None:
    """Parse a player block like '74\\nМухаммеджан Сейсен'.

    The block may contain "number\\nFirstName LastName" or just
    "number FirstName LastName" on a single line.
    """
    # Try multi-line format first: "74\nМухаммеджан Сейсен"
    m = _PLAYER_BLOCK_RE.match(text.strip())
    if m:
        number = int(m.group(1))
        name_part = m.group(2).strip()
    else:
        # Try single-line: "74 Мухаммеджан Сейсен"
        single = re.match(r"^(\d+)\s+(.+)$", text.strip())
        if not single:
            return None
        number = int(single.group(1))
        name_part = single.group(2).strip()

    # Split name: "FirstName LastName" → first, last
    parts = name_part.split(None, 1)
    first_name = parts[0] if parts else ""
    last_name = parts[1] if len(parts) > 1 else ""

    return ParsedPlayer(
        shirt_number=number,
        first_name=first_name,
        last_name=last_name,
    )
