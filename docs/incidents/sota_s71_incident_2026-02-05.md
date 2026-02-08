# SOTA Incident Report: Season 71

- Generated at (UTC): 2026-02-05T22:36:22.936955+00:00
- Mode: dry-run

## Symptoms
- Local no-events matches: 8
- Local duplicate events (signature-based): 0
- Starter anomalies before remediation: `9` / `17`
- Zero-starter matches before remediation: 3

## Evidence
- `em/list` missing rows in CSV: 8
- `em/team` missing ids rows in CSV: 6
- `em/team` missing amplua rows in CSV: 4
- Historical duplicate desync rows in CSV: 2

## Remediation Performed
- Event deduplication: found `0`, deleted `0`
- Lineup rebuild from pre_game_lineup: rebuilt `0`, failed `0` (planned `17`)

## Post-Remediation State
- Local no-events matches: 8 (expected source gap only)
- Local duplicate events: 0
- Starter anomalies after remediation: `9` / `17`
- Zero-starter matches after remediation: 3

## Reproduction Steps
Use a valid SOTA token (`<TOKEN>`) in requests below.
```bash
curl "https://sota.id/em/ddeee5da-7dcb-44f5-8ab8-92beee1fcc26-list.json?access_token=<TOKEN>"
curl "https://sota.id/em/e5a1f8e8-c648-4eab-9e18-68ce62fdea89-team-home.json?access_token=<TOKEN>"
curl "https://sota.id/em/e5a1f8e8-c648-4eab-9e18-68ce62fdea89-team-away.json?access_token=<TOKEN>"
curl "https://sota.id/em/e5a1f8e8-c648-4eab-9e18-68ce62fdea89-list.json?access_token=<TOKEN>"
```

## Impact
- Incomplete/empty source events cause irreversible local gaps without synthetic backfill.
- Missing `id` / `amplua` in `/em/team` payloads can degrade lineup classification.
- Historical mixed sync runs may leave duplicate local events until deduplicated.

## Request to SOTA
1. Restore or provide canonical event payloads for games where `/em/*-list.json` is `404` or empty.
2. Ensure `/em/*-team-{home,away}.json` contains stable player `id` and `amplua` fields.
3. Confirm event-feed retention policy and whether historical edits can remove past event rows.
