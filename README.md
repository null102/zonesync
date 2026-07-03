# ZoneSync

A minimal, high-performance directory mirroring tool.

- **One file.** `zonesync.py`, ~700 lines.
- **Zero dependencies.** Python 3 standard library only.
- **Zero install, zero config.** Drop the file anywhere and run it.
- **One command.** `python3 zonesync.py TARGET`.

ZoneSync borrows the *content index* idea from Git and drops everything
else. There are no commits, branches, merges, history, or repositories —
just:

> Make `TARGET` an exact mirror of the current directory.

---

## Install

There is nothing to install. Copy `zonesync.py` anywhere on your machine.

```bash
curl -O https://.../zonesync.py     # or just save the file
```

Requires Python 3.7+.

---

## Usage

The current working directory is always the **Source of Truth**.
The single CLI argument is the **Target**.

```bash
cd MyProject
python3 zonesync.py /mnt/backup/MyProject
```

Before touching anything, ZoneSync prints a banner and blocks on
`ENTER`:

```
========================================

ZoneSync

Source:
    /home/alice/MyProject

Target:
    /mnt/backup/MyProject

WARNING

Target directory will become an exact mirror of Source.

Files existing only in Target will be permanently deleted.

Press ENTER to continue...
Press Ctrl+C to cancel.

========================================
```

Press `ENTER` to start. Press `Ctrl+C` to abort.

After the sync, `Target == Source` byte-for-byte (excluding the index
file itself, which is copied as its own step so both trees end up warm).

---

## How it works

### Two phases

1. **Analysis.** Walk Source, walk Target, compute a full `SyncPlan`.
   Nothing on disk changes.
2. **Execution.** Apply the plan literally: `mkdir`, `DELETE` (files),
   `COPY`, `UPDATE`, `DELETE` (dirs, deepest first).

### Three-layer change detection

For every file:

1. If the on-disk `(size, mtime)` matches the cached entry in
   `.zonesync.db`, reuse the cached `blake2b` hash. **No re-read.**
2. Otherwise, hash the file with `hashlib.blake2b`.
3. Compare the Source hash and the Target hash. Equal → `SKIP`.
   Different → `UPDATE`.

### The content index (`.zonesync.db`)

A JSON file at the root of each synced tree. It maps *relative* paths to
`(size, mtime, hash, mode)`. It contains:

- no absolute paths
- no source/target identifiers
- no history, UUID, commit, branch, or user info

Because the index is purely relative, moving the tree (e.g. copying a
USB drive to a new mount point) does not invalidate it.

The index is itself copied to Target as a plain file at the end of the
sync. That is what makes the reverse direction fast:

```bash
cd A && python3 zonesync.py B   # first run: hashes everything
cd B && python3 zonesync.py A   # second run: hashes nothing new
```

### Atomic writes

Every copy uses `tmp → flush → fsync → os.replace`. A crash mid-copy
leaves either the old file intact or the new file complete — never a
half-written one.

### Parallelism

Scanning, hashing, and copying share one `ThreadPoolExecutor` sized to
CPU count. `blake2b` and `read`/`write` release the GIL for large chunks,
so this scales well on SSDs.

---

## Log tags

```
[SCAN]     directory walk / hash progress
[PLAN]     analysis results, mkdir warnings
[COPY]     new file placed in Target
[UPDATE]   changed file overwritten in Target
[DELETE]   file or empty directory removed from Target
```

Final summary:

```
========================================
Sync complete
  scanned:     1234
  hashed:      12
  copied:      3
  updated:     1
  deleted:     2
  skipped:     1228
  transferred: 4.72 MiB
  elapsed:     0.31s
========================================
```

---

## What ZoneSync is not

Deliberately absent:

- commits, branches, merges, history
- two-way sync or conflict resolution
- interactive prompts (other than the initial confirmation)
- incremental backups, snapshots, or versioning
- a GUI
- a config file, environment variables, or flags beyond `TARGET`

The tool has exactly one mode: **Source overwrites Target.**

---

## Limitations

- **Symlinks, sockets, devices** are skipped on both sides. Only regular
  files are tracked and mirrored.
- **Permissions** are preserved best-effort (POSIX mode bits only).
  Ownership (uid/gid), ACLs, and extended attributes are not touched.
- **Timestamps** preserved: `mtime`. `atime` and `ctime` are not.
- **Cross-filesystem atomicity** relies on `os.replace`, which is atomic
  within a filesystem but not across mount points. ZoneSync writes the
  temp file next to the destination, so this is normally not an issue.
- **Hash trust.** The `(size, mtime)`-unchanged short-circuit trusts the
  filesystem timestamp. Delete `.zonesync.db` to force a full re-hash.

---

## License

Public domain / do whatever you want.
