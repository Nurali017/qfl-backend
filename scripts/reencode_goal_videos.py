"""Bulk re-encode of existing goal_videos in MinIO to reclaim disk space.

Runs inside qfl-media-worker (kff.kz): downloads each clip, re-encodes with
the chosen preset, and replaces the object under the SAME key, preserving
the original metadata (drive-file-id / game-id) the sync service relies on.
No DB changes — events.video_url stores the object name, which is unchanged.

Safety:
- skips clips already tagged x-amz-meta-reencoded, smaller than MIN_SIZE,
  or where the win is below MIN_WIN (those are tagged via metadata-only copy
  so they are never reprocessed);
- validates output with ffprobe (duration within 2s of source) before upload;
- processes largest-first so disk space on prod frees up fastest;
- one clip at a time; progress in CSV, resumable at any point.

Usage (inside the worker container):
    python /tmp/reencode_goal_videos.py --dry-run     # list what would be done
    python /tmp/reencode_goal_videos.py               # full run
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import time

from minio.commonconfig import CopySource

from app.minio_client import get_minio_client

BUCKET = "qfl-files"
PREFIX = "goal_videos/"
WORK_DIR = "/tmp/reencode"
CSV_PATH = os.path.join(WORK_DIR, "progress.csv")
LOG_PATH = os.path.join(WORK_DIR, "log.txt")

# Chosen via the 3-clip benchmark reviewed by the user (see bench.py results).
ENCODE_ARGS = json.loads(os.environ.get(
    "REENCODE_ARGS",
    '["-c:v","libx264","-preset","slow","-crf","23"]',
))
REENCODED_TAG = os.environ.get("REENCODE_TAG", "crf23_slow")
COMMON_ARGS = ["-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart"]

MIN_SIZE = 20 * 1024 * 1024   # skip clips under 20 MiB
MIN_WIN = 0.10                # require at least 10% size reduction
FFMPEG_TIMEOUT = 30 * 60
DURATION_TOLERANCE_S = 2.0

# Metadata keys the goal-video sync service uses to recognise processed clips.
PRESERVED_META_KEYS = ("drive-file-id", "game-id")


def log(msg: str) -> None:
    line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(LOG_PATH, "a") as f:
        f.write(line + "\n")


def probe_duration(path: str) -> float | None:
    try:
        out = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", path],
            capture_output=True, timeout=60,
        )
        return float(json.loads(out.stdout)["format"]["duration"])
    except Exception:
        return None


def existing_meta(stat) -> dict:
    """Extract the sync-critical metadata from an object stat, normalised."""
    raw = stat.metadata or {}
    meta = {}
    for key in PRESERVED_META_KEYS:
        value = raw.get(f"x-amz-meta-{key}") or raw.get(key)
        if value is not None:
            meta[key] = value
    return meta


def tag_only(client, key: str, meta: dict, reason: str) -> None:
    """Mark an object as processed without re-uploading content."""
    client.copy_object(
        BUCKET, key, CopySource(BUCKET, key),
        metadata={**meta, "reencoded": f"skipped-{reason}"},
        metadata_directive="REPLACE",
    )


def append_csv(row: dict) -> None:
    write_header = not os.path.exists(CSV_PATH)
    with open(CSV_PATH, "a", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["key", "action", "src_mib", "out_mib", "encode_s"],
        )
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def process_clip(client, key: str, size: int, dry_run: bool) -> dict:
    stat = client.stat_object(BUCKET, key)
    raw_meta = stat.metadata or {}
    if raw_meta.get("x-amz-meta-reencoded") or raw_meta.get("reencoded"):
        return {"key": key, "action": "already-done", "src_mib": size // 1024**2,
                "out_mib": size // 1024**2, "encode_s": 0}

    meta = existing_meta(stat)
    if dry_run:
        return {"key": key, "action": "would-encode", "src_mib": size // 1024**2,
                "out_mib": 0, "encode_s": 0}

    src = os.path.join(WORK_DIR, "in.mp4")
    out = os.path.join(WORK_DIR, "out.mp4")
    client.fget_object(BUCKET, key, src)
    src_duration = probe_duration(src)

    t0 = time.time()
    proc = subprocess.run(
        ["ffmpeg", "-y", "-i", src, *ENCODE_ARGS, *COMMON_ARGS, out],
        capture_output=True, timeout=FFMPEG_TIMEOUT,
    )
    elapsed = round(time.time() - t0)
    if proc.returncode != 0:
        stderr = (proc.stderr or b"").decode("utf-8", "replace")[-300:]
        log(f"FFMPEG FAILED {key}: {stderr}")
        return {"key": key, "action": "ffmpeg-error", "src_mib": size // 1024**2,
                "out_mib": 0, "encode_s": elapsed}

    out_size = os.path.getsize(out)
    out_duration = probe_duration(out)
    if (src_duration is None or out_duration is None
            or abs(src_duration - out_duration) > DURATION_TOLERANCE_S):
        log(f"DURATION MISMATCH {key}: src={src_duration} out={out_duration} — keeping original")
        tag_only(client, key, meta, "duration-mismatch")
        return {"key": key, "action": "duration-mismatch", "src_mib": size // 1024**2,
                "out_mib": 0, "encode_s": elapsed}

    if out_size >= size * (1 - MIN_WIN):
        tag_only(client, key, meta, "no-win")
        return {"key": key, "action": "no-win", "src_mib": size // 1024**2,
                "out_mib": out_size // 1024**2, "encode_s": elapsed}

    client.fput_object(
        BUCKET, key, out,
        content_type="video/mp4",
        metadata={**meta, "reencoded": REENCODED_TAG},
    )
    return {"key": key, "action": "replaced", "src_mib": size // 1024**2,
            "out_mib": out_size // 1024**2, "encode_s": elapsed}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0, help="stop after N clips")
    args = parser.parse_args()

    os.makedirs(WORK_DIR, exist_ok=True)
    client = get_minio_client()

    clips = [
        (o.object_name, o.size)
        for o in client.list_objects(BUCKET, prefix=PREFIX, recursive=True)
        if o.size >= MIN_SIZE
    ]
    clips.sort(key=lambda item: -item[1])
    total_gib = sum(s for _, s in clips) / 1024**3
    log(f"queue: {len(clips)} clips >= 20MiB, {total_gib:.1f} GiB total, "
        f"args={ENCODE_ARGS}, dry_run={args.dry_run}")

    saved_bytes = 0
    for index, (key, size) in enumerate(clips, 1):
        if args.limit and index > args.limit:
            break
        try:
            row = process_clip(client, key, size, args.dry_run)
        except Exception as exc:  # noqa: BLE001 — keep the batch going, log per-clip
            log(f"ERROR {key}: {exc!r}")
            row = {"key": key, "action": "error", "src_mib": size // 1024**2,
                   "out_mib": 0, "encode_s": 0}
        append_csv(row)
        if row["action"] == "replaced":
            saved_bytes += (row["src_mib"] - row["out_mib"]) * 1024**2
        if index % 10 == 0 or row["action"] != "already-done":
            log(f"[{index}/{len(clips)}] {row['action']} {key} "
                f"{row['src_mib']}→{row['out_mib']} MiB "
                f"(saved so far: {saved_bytes/1024**3:.1f} GiB)")

    log(f"DONE — saved {saved_bytes/1024**3:.1f} GiB")


if __name__ == "__main__":
    main()
