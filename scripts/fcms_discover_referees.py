"""One-shot discovery: shape of /v1/matches/{id}/matchOfficialAllocations.

Usage:
    python -m scripts.fcms_discover_referees <fcms_match_id> [<fcms_match_id> ...]

Prints raw JSON of matchOfficialAllocations for each match so we can pin
the exact `_embedded` key, role codes, and person fields before coding the sync.
"""

from __future__ import annotations

import asyncio
import json
import sys

from app.services.fcms_client import get_fcms_client


async def _discover(match_ids: list[int]) -> None:
    client = get_fcms_client()
    for mid in match_ids:
        print(f"\n========== match_id={mid} ==========")
        resp = await client._request("GET", f"/v1/matches/{mid}/matchOfficialAllocations", params={"limit": 50})
        data = resp.json()
        print("top-level keys:", list(data.keys()))
        embedded = data.get("_embedded", {}) or {}
        print("_embedded keys:", list(embedded.keys()))
        # Print first item shape per array
        for k, v in embedded.items():
            if isinstance(v, list) and v:
                print(f"\n--- {k}[0] (sample, {len(v)} total) ---")
                print(json.dumps(v[0], indent=2, ensure_ascii=False))
                # Distinct role codes across all items
                role_keys = set()
                for item in v:
                    role = item.get("matchOfficialRole") or item.get("role") or {}
                    if isinstance(role, dict):
                        for code_field in ("shortName", "code", "name"):
                            if code_field in role:
                                role_keys.add(f"{code_field}={role[code_field]}")
                print(f"\ndistinct role keys across {len(v)} items: {sorted(role_keys)}")


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python -m scripts.fcms_discover_referees <fcms_match_id> [...]", file=sys.stderr)
        sys.exit(2)
    match_ids = [int(x) for x in sys.argv[1:]]
    asyncio.run(_discover(match_ids))


if __name__ == "__main__":
    main()
