"""Parse FCMS pre-match and match report PDFs to extract lineups and attendance."""

import re
import logging

import fitz  # PyMuPDF

logger = logging.getLogger(__name__)


def parse_pre_match_lineup(pdf_bytes: bytes) -> dict:
    """Parse pre-match report PDF and extract home/away lineups.

    Returns:
        {
            "home": {"starters": [...], "substitutes": [...]},
            "away": {"starters": [...], "substitutes": [...]},
        }
    Each player dict: {"shirt_number": int, "name": str}

    PDF layout:
        Left column = HOME TEAM, Right column = AWAY TEAM
        Sections: STARTING (11) → players, SUBSTITUTES → players
        Each player line: number followed by name
    """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    full_text = ""
    for page in doc:
        full_text += page.get_text()
    doc.close()

    result = {"home": {"starters": [], "substitutes": []}, "away": {"starters": [], "substitutes": []}}

    # Split into home (first occurrence) and away (second occurrence) sections
    # The PDF has interleaved columns, so we parse sequentially
    # Pattern: STARTING (11) → players → STARTING (11) → players → SUBSTITUTES → players → SUBSTITUTES → players

    lines = full_text.split("\n")
    lines = [l.strip() for l in lines if l.strip()]

    sections: list[tuple[str, list[dict]]] = []  # ("starting"|"substitutes", [players])
    current_section: str | None = None
    current_players: list[dict] = []
    pending_number: int | None = None

    for line in lines:
        # Detect section headers
        if re.match(r"^STARTING\s*\(\d+\)", line):
            if current_section and current_players:
                sections.append((current_section, current_players))
                current_players = []
            current_section = "starting"
            pending_number = None
            continue
        elif re.match(r"^SUBSTITUTES\s*\(\d+\)", line):
            if current_section and current_players:
                sections.append((current_section, current_players))
                current_players = []
            current_section = "substitutes"
            pending_number = None
            continue

        if current_section is None:
            continue

        # Skip header row
        if line in ("#", "Player", "#Player"):
            continue

        # Stop at non-player sections
        if line.startswith("Head Coach:") or line.startswith("OFFICIALS") or line.startswith("SIGNATURES"):
            if current_section and current_players:
                sections.append((current_section, current_players))
                current_players = []
            current_section = None
            continue

        # Try to parse player: first a number line, then a name line
        if pending_number is not None:
            # This line is the player name (may have minute annotations like 61')
            name = re.sub(r"\d+[''′]", "", line).strip()
            if name:
                current_players.append({"shirt_number": pending_number, "name": name})
            pending_number = None
            continue

        # Check if line is a number
        if re.match(r"^\d{1,3}$", line):
            pending_number = int(line)
            continue

        # Some lines might be continuation of previous player name (multi-word)
        if current_players and not re.match(r"^\d", line):
            # Continuation of previous name
            prev = current_players[-1]
            clean = re.sub(r"\d+[''′]", "", line).strip()
            if clean:
                prev["name"] = f'{prev["name"]} {clean}'
            continue

    # Flush last section
    if current_section and current_players:
        sections.append((current_section, current_players))

    # Map sections to home/away: first starting = home, second = away, etc.
    starting_count = 0
    substitutes_count = 0
    for section_type, players in sections:
        if section_type == "starting":
            side = "home" if starting_count == 0 else "away"
            result[side]["starters"] = players
            starting_count += 1
        elif section_type == "substitutes":
            side = "home" if substitutes_count == 0 else "away"
            result[side]["substitutes"] = players
            substitutes_count += 1

    return result


def extract_attendance_from_match_report(pdf_bytes: bytes) -> int | None:
    """Extract attendance number from FCMS match report PDF.

    Looks for "Attendance: <number>" pattern.
    """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    for page in doc:
        text = page.get_text()
        # FCMS format: "Attendance: 1500"
        match = re.search(r"Attendance:\s*(\d[\d\s]*)", text)
        if match:
            doc.close()
            return int(match.group(1).replace(" ", ""))
        # Also try Russian format
        match = re.search(r"Посещаемость:\s*(\d[\d\s]*)", text)
        if match:
            doc.close()
            return int(match.group(1).replace(" ", ""))
    doc.close()
    return None
