"""Video transcoder for goal clips.

Goal-clip originals come straight off stadium cameras — 1080p at 50 fps and
~20 Mbps, which is overkill for a 30-second web replay. We re-encode with
libx264 CRF 20 to cut the file roughly in half without any perceptible
quality loss.

Runs as an out-of-process ``ffmpeg`` call so it doesn't block the event
loop. If transcoding fails for any reason, callers should fall back to the
original file — we never want to lose a clip because of a re-encode hiccup.
"""

from __future__ import annotations

import asyncio
import logging
import os
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

# Variant B — the user-approved preset (see /tmp/qfl-transcode-test benchmark):
#   1080p @ source-fps, CRF 20, preset medium → ~40% smaller than raw camera
#   output, visually indistinguishable.
_DEFAULT_CRF = "20"
_DEFAULT_PRESET = "medium"
_DEFAULT_AUDIO_BITRATE = "128k"
# "0" asks libx264 to auto-pick based on available cores; overridable via
# GOAL_VIDEO_TRANSCODE_THREADS for shared hosts that need a cap.
_DEFAULT_THREADS = "0"
# Per-clip hard cap — well above a normal 30-sec clip transcode on 4 vCPU.
_TRANSCODE_TIMEOUT_SECONDS = 15 * 60


@dataclass(frozen=True)
class TranscodeResult:
    data: bytes
    size: int
    transcoded: bool  # False ⇒ returning the original untouched


@dataclass(frozen=True)
class TranscodePathResult:
    """Path-based transcode outcome — caller reads from `output_path`.

    When `transcoded=False`, `output_path == input_path` (caller should keep
    using the input). When `transcoded=True`, `output_path` is a separate
    file containing the re-encoded clip.
    """
    output_path: Path
    size: int
    transcoded: bool


async def transcode_mp4(
    source_bytes: bytes,
    *,
    crf: str = _DEFAULT_CRF,
    preset: str = _DEFAULT_PRESET,
    audio_bitrate: str = _DEFAULT_AUDIO_BITRATE,
    threads: str = _DEFAULT_THREADS,
    timeout_seconds: int = _TRANSCODE_TIMEOUT_SECONDS,
) -> TranscodeResult:
    """Re-encode a single mp4 payload with ffmpeg. Returns the smaller file
    (or the original, if transcoding would make it larger / fails).
    """
    return await asyncio.to_thread(
        _transcode_sync,
        source_bytes,
        crf=crf,
        preset=preset,
        audio_bitrate=audio_bitrate,
        threads=threads,
        timeout_seconds=timeout_seconds,
    )


async def transcode_mp4_paths(
    input_path: Path,
    output_path: Path,
    *,
    crf: str = _DEFAULT_CRF,
    preset: str = _DEFAULT_PRESET,
    audio_bitrate: str = _DEFAULT_AUDIO_BITRATE,
    threads: str = _DEFAULT_THREADS,
    timeout_seconds: int = _TRANSCODE_TIMEOUT_SECONDS,
) -> TranscodePathResult:
    """Path-to-path variant — no in-memory copies of the clip.

    On success returns the (smaller) `output_path`. On any failure / no-op
    falls back to `input_path` so the caller still has a usable file.
    """
    return await asyncio.to_thread(
        _transcode_paths_sync,
        Path(input_path),
        Path(output_path),
        crf=crf,
        preset=preset,
        audio_bitrate=audio_bitrate,
        threads=threads,
        timeout_seconds=timeout_seconds,
    )


def _transcode_paths_sync(
    input_path: Path,
    output_path: Path,
    *,
    crf: str,
    preset: str,
    audio_bitrate: str,
    threads: str,
    timeout_seconds: int,
) -> TranscodePathResult:
    original_size = input_path.stat().st_size

    def _fallback() -> TranscodePathResult:
        # Caller keeps using the input file; report it as untranscoded.
        if output_path.exists():
            try:
                output_path.unlink()
            except OSError:
                pass
        return TranscodePathResult(
            output_path=input_path, size=original_size, transcoded=False,
        )

    cmd = [
        "ffmpeg", "-y", "-i", str(input_path),
        "-c:v", "libx264",
        "-preset", preset,
        "-crf", crf,
        "-c:a", "aac",
        "-b:a", audio_bitrate,
        "-movflags", "+faststart",
        "-threads", threads,
        str(output_path),
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        logger.warning("ffmpeg transcoder timed out after %ss", timeout_seconds)
        return _fallback()
    except FileNotFoundError:
        logger.warning("ffmpeg binary not found; keeping original clip")
        return _fallback()

    if proc.returncode != 0:
        stderr = (proc.stderr or b"").decode("utf-8", "replace")[-500:]
        logger.warning("ffmpeg exited %d — keeping original. stderr: %s", proc.returncode, stderr)
        return _fallback()

    if not output_path.exists():
        return _fallback()

    transcoded_size = output_path.stat().st_size
    if transcoded_size >= original_size:
        logger.info(
            "Transcode produced %d bytes (original %d) — keeping original",
            transcoded_size, original_size,
        )
        return _fallback()

    logger.info(
        "Transcode OK: %.1f MB → %.1f MB (%.0f%% of original)",
        original_size / 1024 / 1024,
        transcoded_size / 1024 / 1024,
        100 * transcoded_size / original_size,
    )
    return TranscodePathResult(
        output_path=output_path, size=transcoded_size, transcoded=True,
    )


def _transcode_sync(
    source_bytes: bytes,
    *,
    crf: str,
    preset: str,
    audio_bitrate: str,
    threads: str,
    timeout_seconds: int,
) -> TranscodeResult:
    original_size = len(source_bytes)

    tmp_dir = tempfile.mkdtemp(prefix="qfl-transcode-")
    input_path = os.path.join(tmp_dir, "in.mp4")
    output_path = os.path.join(tmp_dir, "out.mp4")

    try:
        with open(input_path, "wb") as f:
            f.write(source_bytes)

        cmd = [
            "ffmpeg", "-y", "-i", input_path,
            "-c:v", "libx264",
            "-preset", preset,
            "-crf", crf,
            "-c:a", "aac",
            "-b:a", audio_bitrate,
            "-movflags", "+faststart",
            "-threads", threads,
            output_path,
        ]
        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                timeout=timeout_seconds,
            )
        except subprocess.TimeoutExpired:
            logger.warning("ffmpeg transcoder timed out after %ss", timeout_seconds)
            return TranscodeResult(data=source_bytes, size=original_size, transcoded=False)
        except FileNotFoundError:
            logger.warning("ffmpeg binary not found; uploading original clip")
            return TranscodeResult(data=source_bytes, size=original_size, transcoded=False)

        if proc.returncode != 0:
            stderr = (proc.stderr or b"").decode("utf-8", "replace")[-500:]
            logger.warning("ffmpeg exited %d — using original. stderr: %s", proc.returncode, stderr)
            return TranscodeResult(data=source_bytes, size=original_size, transcoded=False)

        if not os.path.exists(output_path):
            return TranscodeResult(data=source_bytes, size=original_size, transcoded=False)

        with open(output_path, "rb") as f:
            transcoded = f.read()

        # Safety check — if ffmpeg somehow produced a bigger file, keep the original.
        if len(transcoded) >= original_size:
            logger.info(
                "Transcode produced %d bytes (original %d) — keeping original",
                len(transcoded), original_size,
            )
            return TranscodeResult(data=source_bytes, size=original_size, transcoded=False)

        logger.info(
            "Transcode OK: %.1f MB → %.1f MB (%.0f%% of original)",
            original_size / 1024 / 1024,
            len(transcoded) / 1024 / 1024,
            100 * len(transcoded) / original_size,
        )
        return TranscodeResult(data=transcoded, size=len(transcoded), transcoded=True)
    finally:
        try:
            if os.path.exists(input_path):
                os.remove(input_path)
            if os.path.exists(output_path):
                os.remove(output_path)
            os.rmdir(tmp_dir)
        except Exception:
            logger.debug("Cleanup of %s failed", tmp_dir)
