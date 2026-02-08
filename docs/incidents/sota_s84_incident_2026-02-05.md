# SOTA Incident Report: Season 84

- Generated at (UTC): 2026-02-05T23:23:53.713613+00:00
- Mode: dry-run

## Symptoms
- Local no-events matches: 0
- Local duplicate events (signature-based): 0
- Starter anomalies before remediation: `181` / `182`
- Zero-starter matches before remediation: 121

## Evidence
- `em/list` missing rows in CSV: 0
- `em/team` missing ids rows in CSV: 57
- `em/team` missing amplua rows in CSV: 4
- Historical duplicate desync rows in CSV: 0

## Remediation Performed
- Event deduplication: found `0`, deleted `0`
- Lineup rebuild from pre_game_lineup: rebuilt `0`, failed `0` (planned `182`)

## Post-Remediation State
- Local no-events matches: 0 (expected source gap only)
- Local duplicate events: 0
- Starter anomalies after remediation: `181` / `182`
- Zero-starter matches after remediation: 121

## Reproduction Steps
Use a valid SOTA token (`<TOKEN>`) in requests below.
```bash
curl "https://sota.id/em/9cd01edb-40d8-4914-9e85-1672566896e2-team-home.json?access_token=<TOKEN>"
curl "https://sota.id/em/9cd01edb-40d8-4914-9e85-1672566896e2-team-away.json?access_token=<TOKEN>"
```

## Impact
- Incomplete/empty source events cause irreversible local gaps without synthetic backfill.
- Missing `id` / `amplua` in `/em/team` payloads can degrade lineup classification.
- Historical mixed sync runs may leave duplicate local events until deduplicated.

## Request to SOTA
1. Restore or provide canonical event payloads for games where `/em/*-list.json` is `404` or empty.
2. Ensure `/em/*-team-{home,away}.json` contains stable player `id` and `amplua` fields.
3. Confirm event-feed retention policy and whether historical edits can remove past event rows.
