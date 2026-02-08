# SOTA Incident Report: Season 81

- Generated at (UTC): 2026-02-05T23:08:06.664298+00:00
- Mode: dry-run

## Symptoms
- Local no-events matches: 110
- Local duplicate events (signature-based): 0
- Starter anomalies before remediation: `110` / `110`
- Zero-starter matches before remediation: 98

## Evidence
- `em/list` missing rows in CSV: 110
- `em/team` missing ids rows in CSV: 51
- `em/team` missing amplua rows in CSV: 27
- Historical duplicate desync rows in CSV: 0

## Remediation Performed
- Event deduplication: found `0`, deleted `0`
- Lineup rebuild from pre_game_lineup: rebuilt `0`, failed `0` (planned `110`)

## Post-Remediation State
- Local no-events matches: 110 (expected source gap only)
- Local duplicate events: 0
- Starter anomalies after remediation: `110` / `110`
- Zero-starter matches after remediation: 98

## Reproduction Steps
Use a valid SOTA token (`<TOKEN>`) in requests below.
```bash
curl "https://sota.id/em/2a95f38d-022c-4c3f-9208-8b822d4bda6e-list.json?access_token=<TOKEN>"
curl "https://sota.id/em/ddda1613-11a5-4de9-9bb3-b252eacd91af-team-home.json?access_token=<TOKEN>"
curl "https://sota.id/em/ddda1613-11a5-4de9-9bb3-b252eacd91af-team-away.json?access_token=<TOKEN>"
```

## Impact
- Incomplete/empty source events cause irreversible local gaps without synthetic backfill.
- Missing `id` / `amplua` in `/em/team` payloads can degrade lineup classification.
- Historical mixed sync runs may leave duplicate local events until deduplicated.

## Request to SOTA
1. Restore or provide canonical event payloads for games where `/em/*-list.json` is `404` or empty.
2. Ensure `/em/*-team-{home,away}.json` contains stable player `id` and `amplua` fields.
3. Confirm event-feed retention policy and whether historical edits can remove past event rows.
