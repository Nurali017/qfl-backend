from __future__ import annotations

from dataclasses import dataclass
import ipaddress
import re
from typing import Literal
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Player, Team

DEFAULT_NEWS_BASE_URL = "https://kffleague.kz"
_EXTERNAL_LINK_REL = "noopener noreferrer nofollow"
_UNSAFE_SCHEMES = {"javascript", "vbscript", "data", "file"}
_PLAYER_PATH_MARKERS = {"player", "players"}
_TEAM_PATH_MARKERS = {"team", "teams", "club", "clubs"}
_POSITIVE_INT_RE = re.compile(r"^(\d+)")


@dataclass(frozen=True)
class _LinkCandidate:
    kind: Literal["player", "team"]
    raw_id: int


@dataclass
class NewsHtmlNormalizationResult:
    content: str | None
    links_normalized: int = 0
    external_links_updated: int = 0
    unsafe_links_removed: int = 0
    src_normalized: int = 0
    unsafe_src_removed: int = 0
    player_links_rewritten: int = 0
    team_links_rewritten: int = 0


def _normalize_hostname(hostname: str | None) -> str | None:
    if not hostname:
        return None
    return hostname.lower().strip().rstrip(".")


def _host_variants(hostname: str | None) -> set[str]:
    normalized = _normalize_hostname(hostname)
    if not normalized:
        return set()
    if normalized.startswith("www."):
        return {normalized, normalized[4:]}
    return {normalized, f"www.{normalized}"}


def _looks_internal_hostname(hostname: str | None) -> bool:
    normalized = _normalize_hostname(hostname)
    if not normalized:
        return False
    if normalized in {"localhost", "0.0.0.0", "::1"}:
        return True
    if normalized.endswith(".local") or normalized.startswith("qfl-") or normalized in {"minio", "backend"}:
        return True

    try:
        parsed_ip = ipaddress.ip_address(normalized)
        return parsed_ip.is_private or parsed_ip.is_loopback or parsed_ip.is_link_local
    except ValueError:
        pass

    return "." not in normalized


def _normalize_base_url(base_url: str | None, fallback_base_url: str) -> str:
    for candidate in (base_url, fallback_base_url):
        if not candidate:
            continue
        value = candidate.strip()
        if not value:
            continue
        parsed = urlparse(value)
        if parsed.scheme in {"http", "https"} and parsed.netloc:
            return value

    fallback = fallback_base_url.strip() if fallback_base_url else DEFAULT_NEWS_BASE_URL
    if not fallback.startswith(("http://", "https://")):
        fallback = f"https://{fallback.lstrip('/')}"
    return fallback


def _build_internal_hosts(base_url: str, fallback_base_url: str) -> set[str]:
    hosts: set[str] = set()
    for candidate in (base_url, fallback_base_url, DEFAULT_NEWS_BASE_URL):
        parsed = urlparse(candidate)
        hosts.update(_host_variants(parsed.hostname))
    return hosts


def _base_origin(base_url: str) -> str:
    parsed = urlparse(base_url)
    return f"{parsed.scheme}://{parsed.netloc}"


def _ensure_public_http_url(url: str, *, base_origin: str, internal_hosts: set[str]) -> str:
    parsed = urlparse(url)
    hostname = _normalize_hostname(parsed.hostname)
    if hostname and _looks_internal_hostname(hostname) and hostname not in internal_hosts:
        path = parsed.path or "/"
        if parsed.query:
            path = f"{path}?{parsed.query}"
        if parsed.fragment:
            path = f"{path}#{parsed.fragment}"
        return urljoin(base_origin, path)
    return url


def _parse_positive_int(value: str | None) -> int | None:
    if value is None:
        return None
    match = _POSITIVE_INT_RE.match(value.strip())
    if not match:
        return None
    parsed = int(match.group(1))
    return parsed if parsed > 0 else None


def _normalize_url(
    raw_url: str | None,
    *,
    base_url: str,
    base_origin: str,
    internal_hosts: set[str],
    is_href: bool,
) -> str | None:
    if raw_url is None:
        return None

    value = raw_url.strip()
    if not value:
        return None

    if value.startswith("#"):
        return value if is_href else None
    if value.startswith("//"):
        value = f"https:{value}"

    parsed = urlparse(value)
    scheme = parsed.scheme.lower() if parsed.scheme else ""

    if scheme:
        if scheme in _UNSAFE_SCHEMES:
            return None
        if is_href and scheme in {"mailto", "tel"}:
            return value
        if scheme not in {"http", "https"}:
            return None
        if not parsed.netloc:
            return None
        return _ensure_public_http_url(
            value,
            base_origin=base_origin,
            internal_hosts=internal_hosts,
        )

    normalized = urljoin(base_url, value)
    parsed_normalized = urlparse(normalized)
    if parsed_normalized.scheme not in {"http", "https"} or not parsed_normalized.netloc:
        return None
    return _ensure_public_http_url(
        normalized,
        base_origin=base_origin,
        internal_hosts=internal_hosts,
    )


def _extract_link_candidate(url: str, *, internal_hosts: set[str]) -> _LinkCandidate | None:
    parsed = urlparse(url)
    hostname = _normalize_hostname(parsed.hostname)
    if hostname and hostname not in internal_hosts:
        return None

    segments = [segment for segment in parsed.path.split("/") if segment]
    lowered = [segment.lower() for segment in segments]
    query = parse_qs(parsed.query)

    for index, marker in enumerate(lowered):
        if marker in _PLAYER_PATH_MARKERS and index + 1 < len(segments):
            parsed_id = _parse_positive_int(segments[index + 1])
            if parsed_id is not None:
                return _LinkCandidate(kind="player", raw_id=parsed_id)
        if marker in _TEAM_PATH_MARKERS and index + 1 < len(segments):
            parsed_id = _parse_positive_int(segments[index + 1])
            if parsed_id is not None:
                return _LinkCandidate(kind="team", raw_id=parsed_id)

    for key in ("player_id", "playerId"):
        parsed_id = _parse_positive_int((query.get(key) or [None])[0])
        if parsed_id is not None:
            return _LinkCandidate(kind="player", raw_id=parsed_id)

    for key in ("team_id", "teamId"):
        parsed_id = _parse_positive_int((query.get(key) or [None])[0])
        if parsed_id is not None:
            return _LinkCandidate(kind="team", raw_id=parsed_id)

    if any(marker in _PLAYER_PATH_MARKERS for marker in lowered):
        parsed_id = _parse_positive_int((query.get("id") or [None])[0])
        if parsed_id is not None:
            return _LinkCandidate(kind="player", raw_id=parsed_id)
    if any(marker in _TEAM_PATH_MARKERS for marker in lowered):
        parsed_id = _parse_positive_int((query.get("id") or [None])[0])
        if parsed_id is not None:
            return _LinkCandidate(kind="team", raw_id=parsed_id)

    return None


async def _resolve_player_ids(db: AsyncSession, raw_ids: set[int]) -> dict[int, int]:
    if not raw_ids:
        return {}

    result = await db.execute(
        select(Player.id, Player.legacy_id).where(
            or_(
                Player.id.in_(raw_ids),
                Player.legacy_id.in_(raw_ids),
            )
        )
    )
    rows = result.all()

    matches: dict[int, set[int]] = {raw_id: set() for raw_id in raw_ids}
    for row in rows:
        if row.id in matches:
            matches[row.id].add(row.id)
        if row.legacy_id in matches:
            matches[row.legacy_id].add(row.id)

    return {
        raw_id: next(iter(mapped_ids))
        for raw_id, mapped_ids in matches.items()
        if len(mapped_ids) == 1
    }


async def _resolve_team_ids(db: AsyncSession, raw_ids: set[int]) -> dict[int, int]:
    if not raw_ids:
        return {}

    result = await db.execute(
        select(Team.id, Team.legacy_id).where(
            or_(
                Team.id.in_(raw_ids),
                Team.legacy_id.in_(raw_ids),
            )
        )
    )
    rows = result.all()

    matches: dict[int, set[int]] = {raw_id: set() for raw_id in raw_ids}
    for row in rows:
        if row.id in matches:
            matches[row.id].add(row.id)
        if row.legacy_id in matches:
            matches[row.legacy_id].add(row.id)

    return {
        raw_id: next(iter(mapped_ids))
        for raw_id, mapped_ids in matches.items()
        if len(mapped_ids) == 1
    }


def _is_external_href(href: str, *, internal_hosts: set[str]) -> bool:
    lowered = href.lower()
    if lowered.startswith(("/", "#", "mailto:", "tel:")):
        return False
    parsed = urlparse(href)
    if parsed.scheme in {"http", "https"}:
        hostname = _normalize_hostname(parsed.hostname)
        return hostname not in internal_hosts
    return False


def _to_relative_if_internal(url: str, *, internal_hosts: set[str]) -> str:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return url
    hostname = _normalize_hostname(parsed.hostname)
    if hostname not in internal_hosts:
        return url

    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"
    if parsed.fragment:
        path = f"{path}#{parsed.fragment}"
    return path


def _render_soup_fragment(soup: BeautifulSoup) -> str:
    if soup.body is None:
        return str(soup)
    return "".join(str(child) for child in soup.body.contents)


def normalize_news_media_url(
    media_url: str | None,
    *,
    source_url: str | None,
    fallback_base_url: str = DEFAULT_NEWS_BASE_URL,
) -> str | None:
    base_url = _normalize_base_url(source_url, fallback_base_url)
    base_origin = _base_origin(base_url)
    internal_hosts = _build_internal_hosts(base_url, fallback_base_url)
    return _normalize_url(
        media_url,
        base_url=base_url,
        base_origin=base_origin,
        internal_hosts=internal_hosts,
        is_href=False,
    )


async def normalize_news_html_content(
    content: str | None,
    *,
    source_url: str | None,
    db: AsyncSession,
    fallback_base_url: str = DEFAULT_NEWS_BASE_URL,
) -> NewsHtmlNormalizationResult:
    result = NewsHtmlNormalizationResult(content=content)
    if not content:
        return result

    base_url = _normalize_base_url(source_url, fallback_base_url)
    base_origin = _base_origin(base_url)
    internal_hosts = _build_internal_hosts(base_url, fallback_base_url)

    soup = BeautifulSoup(content, "lxml")
    mutated = False

    anchors = list(soup.find_all("a"))
    candidates: dict[int, _LinkCandidate] = {}
    player_candidate_ids: set[int] = set()
    team_candidate_ids: set[int] = set()

    for index, anchor in enumerate(anchors):
        raw_href = anchor.get("href")
        normalized_href = _normalize_url(
            raw_href,
            base_url=base_url,
            base_origin=base_origin,
            internal_hosts=internal_hosts,
            is_href=True,
        )
        if normalized_href is None:
            if anchor.has_attr("href"):
                del anchor["href"]
                result.unsafe_links_removed += 1
                mutated = True
            anchor.attrs.pop("target", None)
            anchor.attrs.pop("rel", None)
            continue

        if raw_href != normalized_href:
            result.links_normalized += 1
            mutated = True
        anchor["href"] = normalized_href

        candidate = _extract_link_candidate(normalized_href, internal_hosts=internal_hosts)
        if candidate:
            candidates[index] = candidate
            if candidate.kind == "player":
                player_candidate_ids.add(candidate.raw_id)
            else:
                team_candidate_ids.add(candidate.raw_id)

    player_map = await _resolve_player_ids(db, player_candidate_ids)
    team_map = await _resolve_team_ids(db, team_candidate_ids)

    for index, anchor in enumerate(anchors):
        href = anchor.get("href")
        if not href:
            continue

        candidate = candidates.get(index)
        rewritten_href: str | None = None
        if candidate and candidate.kind == "player":
            mapped_player_id = player_map.get(candidate.raw_id)
            if mapped_player_id is not None:
                rewritten_href = f"/player/{mapped_player_id}"
                result.player_links_rewritten += 1
        elif candidate and candidate.kind == "team":
            mapped_team_id = team_map.get(candidate.raw_id)
            if mapped_team_id is not None:
                rewritten_href = f"/team/{mapped_team_id}"
                result.team_links_rewritten += 1

        next_href = rewritten_href or _to_relative_if_internal(href, internal_hosts=internal_hosts)
        if next_href != href:
            result.links_normalized += 1
            mutated = True
        anchor["href"] = next_href

        is_external = _is_external_href(next_href, internal_hosts=internal_hosts)
        if is_external:
            previous_target = anchor.get("target")
            previous_rel = anchor.get("rel")
            previous_rel_value = " ".join(previous_rel) if isinstance(previous_rel, list) else (previous_rel or "")
            if previous_target != "_blank" or previous_rel_value != _EXTERNAL_LINK_REL:
                result.external_links_updated += 1
                mutated = True
            anchor["target"] = "_blank"
            anchor["rel"] = _EXTERNAL_LINK_REL
        else:
            if anchor.has_attr("target") or anchor.has_attr("rel"):
                mutated = True
            anchor.attrs.pop("target", None)
            anchor.attrs.pop("rel", None)

    for tag_name in ("img", "source", "iframe"):
        for node in soup.find_all(tag_name):
            raw_src = node.get("src")
            if raw_src is None:
                continue

            normalized_src = _normalize_url(
                raw_src,
                base_url=base_url,
                base_origin=base_origin,
                internal_hosts=internal_hosts,
                is_href=False,
            )
            if normalized_src is None:
                del node["src"]
                result.unsafe_src_removed += 1
                mutated = True
                continue

            if raw_src != normalized_src:
                result.src_normalized += 1
                mutated = True
            node["src"] = normalized_src

    if mutated:
        result.content = _render_soup_fragment(soup)
    return result

