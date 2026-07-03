#!/usr/bin/env python3
"""ZoneSync -- a minimal, high-performance directory mirroring tool.

Usage
-----
    python3 zonesync.py TARGET

The current working directory is always the Source of Truth. ``TARGET``
becomes an exact mirror of Source after the sync completes: new files are
copied, changed files are updated, and files that exist only in TARGET are
deleted.

Design summary
--------------
* **Content index** (``.zonesync.db``) -- a JSON file at the root of each
  synced tree that maps *relative* paths to ``(size, mtime, hash, mode)``.
  Because paths are relative, the index stays valid when the whole tree is
  moved.
* **Three-layer change detection**:
    1. If on-disk ``(size, mtime)`` matches the cached entry, the cached
       ``blake2b`` hash is reused verbatim (no re-read).
    2. Otherwise the file is re-hashed with ``hashlib.blake2b``.
    3. Files whose hashes match between Source and Target are skipped.
* **Two phases**: Phase 1 (Analysis) produces a full ``SyncPlan`` without
  touching either tree. Phase 2 (Execution) applies the plan.
* **Atomic writes**: every file lands via ``tmp -> flush -> fsync -> rename``.
* **Parallelism**: scanning/hashing/copying run in a shared
  ``ThreadPoolExecutor``.

This is not Git. There are no commits, branches, merges, or history --
just "make Target look like Source".
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import shutil
import stat
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


# -------------------------------------------------------------------
# Constants
# -------------------------------------------------------------------

INDEX_FILENAME: str = ".zonesync.db"
"""Name of the per-tree content-index file, stored at the tree root."""

INDEX_VERSION: int = 1
"""Bumped whenever the on-disk index format changes."""

HASH_CHUNK_SIZE: int = 1 << 20  # 1 MiB
"""Chunk size used when hashing or copying file contents."""

MTIME_EPSILON: float = 1e-6
"""Tolerance when comparing floating-point mtimes."""

DEFAULT_WORKERS: int = max(4, (os.cpu_count() or 4))
"""Thread-pool size used for scanning, hashing, and copying."""


# -------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------

_log_lock = threading.Lock()


def log(tag: str, message: str) -> None:
    """Emit a thread-safe ``[TAG] message`` line to stdout."""
    with _log_lock:
        sys.stdout.write(f"[{tag}] {message}\n")
        sys.stdout.flush()


# -------------------------------------------------------------------
# Data structures
# -------------------------------------------------------------------

@dataclass(frozen=True)
class FileEntry:
    """Cached metadata for a single tracked file.

    ``mode`` stores the POSIX permission bits so they can be restored on
    the target side (best-effort; ignored on Windows).
    """
    size: int
    mtime: float
    hash: str
    mode: int

    def to_json(self) -> Dict[str, object]:
        return {"size": self.size, "mtime": self.mtime,
                "hash": self.hash, "mode": self.mode}

    @classmethod
    def from_json(cls, data: Dict[str, object]) -> "FileEntry":
        return cls(
            size=int(data["size"]),
            mtime=float(data["mtime"]),
            hash=str(data["hash"]),
            mode=int(data.get("mode", 0o644)),
        )


@dataclass
class DirState:
    """Snapshot of one directory tree: files (with hashes) and its dir set."""
    files: Dict[str, FileEntry] = field(default_factory=dict)
    dirs: Set[str] = field(default_factory=set)


@dataclass
class SyncPlan:
    """The full set of operations that will turn Target into Source."""
    mkdirs: List[str] = field(default_factory=list)
    copies: List[str] = field(default_factory=list)
    updates: List[str] = field(default_factory=list)
    file_deletes: List[str] = field(default_factory=list)
    dir_deletes: List[str] = field(default_factory=list)
    skips: int = 0


@dataclass
class Stats:
    """Aggregate counters printed in the final summary."""
    scanned: int = 0
    hashed: int = 0
    copied: int = 0
    updated: int = 0
    deleted: int = 0
    skipped: int = 0
    bytes_transferred: int = 0
    started: float = 0.0
    finished: float = 0.0

    def elapsed(self) -> float:
        return max(0.0, self.finished - self.started)


# -------------------------------------------------------------------
# Hashing
# -------------------------------------------------------------------

def hash_file(path: Path) -> str:
    """Return the ``blake2b`` digest of ``path`` as a hex string (streamed)."""
    h = hashlib.blake2b()
    with path.open("rb") as f:
        while True:
            chunk = f.read(HASH_CHUNK_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


# -------------------------------------------------------------------
# Content index (.zonesync.db)
# -------------------------------------------------------------------

def load_index(root: Path) -> Dict[str, FileEntry]:
    """Load ``root/.zonesync.db``. Return ``{}`` if missing or corrupt."""
    idx_path = root / INDEX_FILENAME
    if not idx_path.is_file():
        return {}
    try:
        with idx_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}
    if (not isinstance(data, dict)) or data.get("version") != INDEX_VERSION:
        return {}
    files_obj = data.get("files") or {}
    result: Dict[str, FileEntry] = {}
    if isinstance(files_obj, dict):
        for rel, entry in files_obj.items():
            if not isinstance(entry, dict):
                continue
            try:
                result[str(rel)] = FileEntry.from_json(entry)
            except (KeyError, TypeError, ValueError):
                continue
    return result


def save_index(root: Path, files: Dict[str, FileEntry]) -> None:
    """Atomically overwrite ``root/.zonesync.db`` with ``files``."""
    idx_path = root / INDEX_FILENAME
    payload = {
        "version": INDEX_VERSION,
        "files": {rel: entry.to_json() for rel, entry in sorted(files.items())},
    }
    data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    atomic_write_bytes(idx_path, data, mode=0o644)


# -------------------------------------------------------------------
# Atomic write / copy
# -------------------------------------------------------------------

def _tmp_path_for(dest: Path) -> Path:
    """Return a sibling temp path used for the atomic-rename dance."""
    suffix = f".zonesync-tmp.{os.getpid()}.{threading.get_ident()}"
    return dest.parent / (dest.name + suffix)


def atomic_write_bytes(dest: Path, data: bytes, *, mode: int) -> None:
    """Write ``data`` to ``dest`` atomically (tmp -> fsync -> rename)."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp_path_for(dest)
    try:
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                pass
        try:
            os.chmod(tmp, mode)
        except OSError:
            pass
        os.replace(tmp, dest)
    except BaseException:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass
        raise


def atomic_copy(src: Path, dest: Path, *, mtime: float, mode: int) -> int:
    """Copy ``src`` to ``dest`` atomically, preserving mtime and mode.

    Returns the number of bytes copied.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp_path_for(dest)
    n = 0
    try:
        with src.open("rb") as sf, open(tmp, "wb") as df:
            while True:
                chunk = sf.read(HASH_CHUNK_SIZE)
                if not chunk:
                    break
                df.write(chunk)
                n += len(chunk)
            df.flush()
            try:
                os.fsync(df.fileno())
            except OSError:
                pass
        try:
            os.chmod(tmp, mode)
        except OSError:
            pass
        try:
            os.utime(tmp, (mtime, mtime))
        except OSError:
            pass
        os.replace(tmp, dest)
    except BaseException:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass
        raise
    return n


# -------------------------------------------------------------------
# Scanning
# -------------------------------------------------------------------

def scan_directory(
    root: Path,
    cached: Dict[str, FileEntry],
    executor: concurrent.futures.Executor,
    stats: Stats,
    label: str,
) -> DirState:
    """Walk ``root`` and build a :class:`DirState`.

    Layer 1 short-circuit: whenever the on-disk ``(size, mtime)`` matches the
    cached entry, the cached hash is reused directly. Otherwise the file is
    queued for parallel ``blake2b`` hashing on ``executor``. The root-level
    ``.zonesync.db`` file is deliberately excluded from the tracked set --
    it is synced as a plain post-execution step.
    """
    log("SCAN", f"walking {label}: {root}")

    state = DirState()
    to_hash: List[Tuple[str, Path, os.stat_result]] = []

    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        dpath = Path(dirpath)
        try:
            rel_dir = dpath.relative_to(root)
        except ValueError:
            continue
        rel_dir_str = "" if rel_dir == Path(".") else rel_dir.as_posix()
        if rel_dir_str:
            state.dirs.add(rel_dir_str)

        for name in filenames:
            # Never track the root-level index file itself.
            if not rel_dir_str and name == INDEX_FILENAME:
                continue

            rel = f"{rel_dir_str}/{name}" if rel_dir_str else name
            fpath = dpath / name

            try:
                st = fpath.lstat()
            except OSError:
                continue
            # Symlinks / devices / sockets are outside the sync's scope.
            if not stat.S_ISREG(st.st_mode):
                continue

            mode = stat.S_IMODE(st.st_mode)
            cached_entry = cached.get(rel)
            if (cached_entry is not None
                    and cached_entry.size == st.st_size
                    and abs(cached_entry.mtime - st.st_mtime) < MTIME_EPSILON):
                # Layer 1: metadata match -> reuse cached hash.
                state.files[rel] = FileEntry(
                    size=st.st_size, mtime=st.st_mtime,
                    hash=cached_entry.hash, mode=mode,
                )
            else:
                # Layer 2: needs a fresh blake2b hash.
                to_hash.append((rel, fpath, st))

    if to_hash:
        def task(item: Tuple[str, Path, os.stat_result]
                 ) -> Optional[Tuple[str, FileEntry]]:
            rel, fpath, st = item
            try:
                digest = hash_file(fpath)
            except OSError as exc:
                log("SCAN", f"unreadable, skipping: {rel} ({exc})")
                return None
            return rel, FileEntry(
                size=st.st_size, mtime=st.st_mtime, hash=digest,
                mode=stat.S_IMODE(st.st_mode),
            )

        for result in executor.map(task, to_hash):
            if result is None:
                continue
            rel, entry = result
            state.files[rel] = entry
            stats.hashed += 1

    stats.scanned += len(state.files)
    log("SCAN", f"{label}: {len(state.files)} files, {len(state.dirs)} dirs "
                f"({len(to_hash)} hashed, "
                f"{len(state.files) - min(len(to_hash), len(state.files))} cached)")
    return state


# -------------------------------------------------------------------
# Diff (source vs target -> SyncPlan)
# -------------------------------------------------------------------

def _dir_depth(rel: str) -> int:
    return rel.count("/")


def compute_plan(source: DirState, target: DirState) -> SyncPlan:
    """Compute the minimal ``SyncPlan`` that turns ``target`` into ``source``.

    Files are matched by relative path; content equality is by blake2b hash.
    Directories on either side are reconciled via ``mkdir``/``rmdir`` so
    empty source directories are preserved and stray target directories go
    away.
    """
    plan = SyncPlan()

    src_rels = set(source.files.keys())
    tgt_rels = set(target.files.keys())

    plan.mkdirs = sorted(source.dirs - target.dirs)
    plan.dir_deletes = sorted(target.dirs - source.dirs,
                              key=lambda p: (-_dir_depth(p), p))

    plan.copies = sorted(src_rels - tgt_rels)
    plan.file_deletes = sorted(tgt_rels - src_rels)

    for rel in sorted(src_rels & tgt_rels):
        # Layer 3: hash equality decides whether we can skip.
        if source.files[rel].hash == target.files[rel].hash:
            plan.skips += 1
        else:
            plan.updates.append(rel)
    return plan


# -------------------------------------------------------------------
# Execution
# -------------------------------------------------------------------

def execute_plan(
    source_root: Path,
    target_root: Path,
    plan: SyncPlan,
    source_state: DirState,
    executor: concurrent.futures.Executor,
    stats: Stats,
) -> None:
    """Run ``plan`` against ``target_root``.

    Order of operations:
        1. Create source-only directories (so empty dirs are mirrored too).
        2. Delete files that exist only in target.
        3. Copy new files and update changed files (parallel).
        4. Remove target-only directories, deepest first.
    """
    # 1. Create missing directories.
    for rel in plan.mkdirs:
        try:
            (target_root / rel).mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            log("PLAN", f"mkdir failed: {rel} ({exc})")

    # 2. Delete files only in target.
    for rel in plan.file_deletes:
        p = target_root / rel
        try:
            p.unlink()
            stats.deleted += 1
            log("DELETE", rel)
        except OSError as exc:
            log("DELETE", f"FAILED: {rel} ({exc})")

    # 3. Copies + updates in parallel.
    def copy_task(rel: str, tag: str
                  ) -> Tuple[str, str, int, Optional[str]]:
        entry = source_state.files[rel]
        src = source_root / rel
        dst = target_root / rel
        try:
            n = atomic_copy(src, dst, mtime=entry.mtime, mode=entry.mode)
        except OSError as exc:
            return rel, tag, 0, str(exc)
        return rel, tag, n, None

    futures: List[concurrent.futures.Future] = []
    for rel in plan.copies:
        futures.append(executor.submit(copy_task, rel, "COPY"))
    for rel in plan.updates:
        futures.append(executor.submit(copy_task, rel, "UPDATE"))

    for fut in concurrent.futures.as_completed(futures):
        rel, tag, nbytes, err = fut.result()
        if err is not None:
            log(tag, f"FAILED: {rel} ({err})")
            continue
        log(tag, rel)
        stats.bytes_transferred += nbytes
        if tag == "COPY":
            stats.copied += 1
        else:
            stats.updated += 1

    # 4. Remove target-only directories (deepest first).
    for rel in plan.dir_deletes:
        p = target_root / rel
        if not p.exists():
            continue
        try:
            p.rmdir()
            log("DELETE", f"{rel}/")
        except OSError:
            # Non-empty (e.g. contained untracked cruft) -> recursive remove.
            try:
                shutil.rmtree(p)
                log("DELETE", f"{rel}/ (recursive)")
            except OSError as exc:
                log("DELETE", f"FAILED: {rel}/ ({exc})")

    stats.skipped += plan.skips


def _sync_root_index(source_root: Path, target_root: Path) -> None:
    """Copy Source's ``.zonesync.db`` to Target as a plain file.

    The index file is excluded from the normal plan (its content changes
    every run), so we copy it explicitly after execution finishes. This is
    what lets ``A -> B`` and ``B -> A`` runs skip re-hashing on the second
    call.
    """
    src = source_root / INDEX_FILENAME
    dst = target_root / INDEX_FILENAME
    if not src.is_file():
        return
    try:
        data = src.read_bytes()
        st = src.stat()
        atomic_write_bytes(dst, data, mode=stat.S_IMODE(st.st_mode))
        try:
            os.utime(dst, (st.st_mtime, st.st_mtime))
        except OSError:
            pass
        log("COPY", INDEX_FILENAME)
    except OSError as exc:
        log("COPY", f"{INDEX_FILENAME} failed: {exc}")


# -------------------------------------------------------------------
# Confirmation banner
# -------------------------------------------------------------------

BANNER = """\
========================================

ZoneSync

Source:
    {source}

Target:
    {target}

WARNING

Target directory will become an exact mirror of Source.

Files existing only in Target will be permanently deleted.

Press ENTER to continue...
Press Ctrl+C to cancel.

========================================
"""


def prompt_confirmation(source: Path, target: Path) -> None:
    """Show the warning banner and block on ENTER (or abort on Ctrl+C / EOF)."""
    sys.stdout.write(BANNER.format(source=source, target=target))
    sys.stdout.flush()
    try:
        input()
    except (KeyboardInterrupt, EOFError):
        sys.stderr.write("\nAborted.\n")
        raise SystemExit(130)


# -------------------------------------------------------------------
# CLI / main
# -------------------------------------------------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse the single positional TARGET argument."""
    p = argparse.ArgumentParser(
        prog="zonesync",
        description="Mirror the current directory to a target directory.",
    )
    p.add_argument("target", help="target directory (will be overwritten)")
    return p.parse_args(argv)


def _is_inside(child: Path, parent: Path) -> bool:
    """True if ``child`` is (or is nested under) ``parent``."""
    try:
        child.relative_to(parent)
        return True
    except ValueError:
        return False


def format_bytes(n: int) -> str:
    """Human-readable byte count (SI-style binary units)."""
    size = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1024 or unit == "TiB":
            if unit == "B":
                return f"{int(size)} B"
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PiB"


def print_summary(stats: Stats) -> None:
    """Print the final stats block."""
    sys.stdout.write("\n")
    sys.stdout.write("========================================\n")
    sys.stdout.write("Sync complete\n")
    sys.stdout.write(f"  scanned:     {stats.scanned}\n")
    sys.stdout.write(f"  hashed:      {stats.hashed}\n")
    sys.stdout.write(f"  copied:      {stats.copied}\n")
    sys.stdout.write(f"  updated:     {stats.updated}\n")
    sys.stdout.write(f"  deleted:     {stats.deleted}\n")
    sys.stdout.write(f"  skipped:     {stats.skipped}\n")
    sys.stdout.write(f"  transferred: {format_bytes(stats.bytes_transferred)}\n")
    sys.stdout.write(f"  elapsed:     {stats.elapsed():.2f}s\n")
    sys.stdout.write("========================================\n")
    sys.stdout.flush()


def main(argv: Optional[List[str]] = None) -> int:
    """Program entry point. Returns the process exit code."""
    args = parse_args(argv)

    source = Path.cwd().resolve()

    target_arg = Path(args.target)
    try:
        if not target_arg.exists():
            target_arg.mkdir(parents=True, exist_ok=True)
        target = target_arg.resolve()
    except OSError as exc:
        sys.stderr.write(f"error: cannot access target: {exc}\n")
        return 2

    if not target.is_dir():
        sys.stderr.write(f"error: target is not a directory: {target}\n")
        return 2
    if source == target:
        sys.stderr.write("error: source and target are the same directory\n")
        return 2
    if _is_inside(target, source):
        sys.stderr.write(f"error: target is inside source: {target}\n")
        return 2
    if _is_inside(source, target):
        sys.stderr.write(f"error: source is inside target: {source}\n")
        return 2

    prompt_confirmation(source, target)

    stats = Stats()
    stats.started = time.monotonic()

    try:
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=DEFAULT_WORKERS) as executor:
            # ---- Phase 1: Analysis (no destructive writes) ----------
            source_cache = load_index(source)
            source_state = scan_directory(
                source, source_cache, executor, stats, "source")
            # Persist Source's index so future runs can reuse hashes even
            # if this run is interrupted before Execution finishes.
            try:
                save_index(source, source_state.files)
            except OSError as exc:
                log("PLAN", f"warning: cannot write source index: {exc}")

            target_cache = load_index(target)
            target_state = scan_directory(
                target, target_cache, executor, stats, "target")

            log("PLAN", "diffing source vs target")
            plan = compute_plan(source_state, target_state)
            log("PLAN",
                f"copy={len(plan.copies)} update={len(plan.updates)} "
                f"delete={len(plan.file_deletes)} skip={plan.skips} "
                f"mkdir={len(plan.mkdirs)} rmdir={len(plan.dir_deletes)}")

            # ---- Phase 2: Execution ---------------------------------
            execute_plan(source, target, plan, source_state, executor, stats)

            # Finally, mirror Source's index onto Target so the reverse
            # direction ("cd Target && python3 zonesync.py Source") also
            # starts warm.
            _sync_root_index(source, target)
    except KeyboardInterrupt:
        sys.stderr.write("\nInterrupted.\n")
        stats.finished = time.monotonic()
        print_summary(stats)
        return 130

    stats.finished = time.monotonic()
    print_summary(stats)
    return 0


if __name__ == "__main__":
    sys.exit(main())


# -------------------------------------------------------------------
# Design notes (kept alongside the code so the intent stays discoverable)
# -------------------------------------------------------------------
#
# * "Target becomes Source" is the *only* contract. No two-way merges, no
#   conflict detection, no history. If Source loses a file, Target loses
#   it too on the next run -- that is the entire feature set.
#
# * The content index (``.zonesync.db``) is a *cache*, not authority. Its
#   only job is to make ``(size, mtime)``-unchanged files avoid a re-read.
#   If it is missing, corrupt, or stale, correctness is unaffected -- we
#   fall through to a full re-hash. Deleting it costs performance, never
#   correctness.
#
# * Because the index stores *relative* paths only, moving or renaming
#   the whole tree (e.g. copying a USB drive to a new mount point) does
#   not invalidate the cache. This is the property that makes ``A -> B``
#   and ``B -> A`` symmetric in wall-clock cost after the first sync.

