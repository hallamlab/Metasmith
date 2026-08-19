"""A library whose recorded identities are taken at their word.

A `DataInstanceLibrary` normally derives a leaf's `instance_id` from the file's
bytes at `AddItem` time. That is the right default and it is what makes two
independent runs over identical inputs hit the same cache shards. It is the
wrong default for a set of reference databases: they are transform products with
no inputs, they do not change, and re-deriving their identity costs 10 seconds
of blake3 over 24 GB on *every* plan against a solve that takes 1.

Freezing is how a library says its recorded ids are already correct. A frozen
library refuses every mutation, returns `instance_meta` entries verbatim without
consulting the filesystem, and records a cheap witness -- a stat stamp per
top-level entry -- that `Load` checks so a library whose bytes visibly moved
raises instead of silently serving an id that no longer describes them.

## What the two cheap mechanisms do NOT catch

Read this before trusting either one, and before changing either one. They are
a smoke alarm, not a lock. The failure direction is asymmetric and bad: an
undetected content swap under an unchanged id is a false cache *hit*, which
replays a stale shard and produces silently wrong scientific output with no
error anywhere. That asymmetry is why this is written down rather than
reassured about.

**The read-only mark (`chmod a-w`, top-level entries only) does not reach:**

- *Inside a directory entry.* The mark is deliberately non-recursive --
  `kofam_ref/profiles` holds 27,756 files and walking them would reintroduce the
  cost this exists to remove. Clearing write on a directory blocks creating,
  deleting and renaming entries in it; it does not block editing a file already
  inside, and nested files keep whatever mode they had.
- *Past the owner*, who can chmod it back, or *root*, who ignores it. It stops
  accident, never intent.
- *Past a legitimate `dvc checkout` of a different pin*, which swaps the bytes
  and restores mode 444. The mode is unchanged and the content is not; nothing
  about the mark notices.
- *Past any caller that constructs a `DataInstanceLibrary` directly* rather than
  going through this API, or that `rmtree`s the location. The refusals bind
  callers, not the filesystem.
- *Onto a remote agent.* Staging re-applies no modes, and mode preservation
  across copy paths and filesystems is best effort.
- *Where there is no local file at all* -- a library of remote paths has nothing
  to mark.
- It can also *break* a later `dvc checkout` or `dvc pull` with EACCES on the
  read-only directory. That failure is loud, which is the acceptable half of the
  trade; unfreeze first.

**The stat stamp `(size, mtime_ns)` does not reach:**

- *mtime is not content.* A same-size in-place edit that preserves mtime
  (`cp -p`, `rsync --times`, `tar -p`, `touch -r`) passes undetected.
- *Directories are the weak case, and they are the entries that most need it.* A
  directory's mtime reflects only its own entry list and its inode size means
  nothing, so a change nested inside one is invisible. The immediate-entry count
  is recorded to make the stamp less vacuous; it is still weak.
- *Across hosts.* mtime granularity and clock skew on the NFS/Lustre filesystems
  the HPC copies live on make stamps non-comparable, so a stamp taken on another
  host warns rather than raises. Honest coverage is "the machine that froze it".
- *Tamper evidence.* The stamp lives in the file it validates and re-freezing
  silently re-stamps. This is a consistency check, not an integrity check.

The likelier day-to-day failure is the **false positive**: re-materialising the
same DVC pin moves mtime, and the bytes are fine. `Restamp()` is the remedy --
it re-records stamps and moves no `instance_id`. Turning the check off is not
the remedy, which is why there is a verb for this and the kill switch
(`METASMITH_FROZEN_NOCHECK=1`) is documented as an emergency, not a fix.

**The escape hatch with none of these holes** is `metasmith data verify --deep`,
which re-derives real content digests and compares them against what `freeze
--deep` recorded. Expensive, never automatic: run it before a release or after a
cache hit you did not expect. Where no `--deep` baseline exists it reports
`UNVERIFIABLE`, never `OK` -- the tool must not launder "we did not check" into
"it is fine".

## Why this is not the shortcut `docs/metasmith/plans/cross-run-reentrancy.md` rejected

That document rejected `(size, mtime)` as the *derivation* of identity, on the
premise that hashing is a one-time build cost that amortizes to zero. Nothing
here derives an identity from a stamp. The id comes from content (or, for the
fabfos references, from the DVC pin's md5, which is itself a digest over the
bytes); the stamp only raises a question about an id that already exists, and
fails closed by raising. The rejection stands; this is a different mechanism at
a different point in the pipeline. The premise it rested on is also what this
change repairs -- the driver was re-paying that "one-time" cost per plan.
"""

from __future__ import annotations

import os
import socket
import stat as stat_mod
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from ...logging import Log


class FrozenLibraryError(RuntimeError):
    """A refusal by a frozen library, or a stamp that no longer matches."""


#: Emergency only. Documented in the module docstring as *not* the remedy for a
#: false positive -- `Restamp()` is. Present because a stamp that raises on a
#: cluster at 3am must be defeatable by someone who cannot edit the index.
_NOCHECK_ENV = "METASMITH_FROZEN_NOCHECK"


def _stamp(abs_path: Path) -> dict:
    """A cheap witness that `abs_path` has not visibly moved.

    One `stat` for a file. For a directory, one `stat` plus one `listdir` of the
    immediate entries -- see the module docstring for exactly how little that
    proves.
    """
    st = abs_path.stat()
    if stat_mod.S_ISDIR(st.st_mode):
        try:
            n = len(os.listdir(abs_path))
        except OSError:
            n = -1
        return {"kind": "dir", "mtime_ns": st.st_mtime_ns, "n_entries": n}
    return {"kind": "file", "size": st.st_size, "mtime_ns": st.st_mtime_ns}


def _content_digest(abs_path: Path) -> str | None:
    """A real digest of the bytes. Expensive; only `--deep` asks for it."""
    from ...caching.keys import content_multihash_key, tree_multihash_key

    try:
        if abs_path.is_dir():
            return tree_multihash_key(abs_path).hex()
        return content_multihash_key(abs_path).hex()
    except OSError:
        return None


def _stamp_fields(entry: dict) -> set[str]:
    """Just the witness, not the bookkeeping recorded beside it."""
    return {"kind", "size", "mtime_ns", "n_entries"} & set(entry)


def _describe(recorded: dict, observed: dict) -> str:
    keys = sorted(set(recorded) | set(observed))
    parts = [
        f"{k}: recorded={recorded.get(k)!r} observed={observed.get(k)!r}"
        for k in keys
        if recorded.get(k) != observed.get(k)
    ]
    return "; ".join(parts) or "no visible difference"


class _FrozenLibrary:
    #: The raw `frozen:` block from `index.yml`, or None. Absent means not
    #: frozen, which is why every library written before this existed keeps
    #: loading unchanged.
    _frozen: dict | None = None

    @property
    def is_frozen(self) -> bool:
        return self._frozen is not None

    def _refuse_if_frozen(self, verb: str) -> None:
        if not self.is_frozen:
            return
        raise FrozenLibraryError(
            f"[{verb}] refused: the library at [{self.location}] is frozen."
            " Its recorded instance_ids are what downstream cache keys are built"
            " from, so changing it here would re-key every run that used it."
            " Unfreeze deliberately (`metasmith data unfreeze`) if that is what"
            " you mean."
        )

    def _abs(self, path: Path) -> Path:
        return path if path.is_absolute() else self.location / path

    def Freeze(
        self,
        *,
        apply_permissions: bool = True,
        provenance: dict[Path, dict] | None = None,
        deep: bool = False,
    ) -> dict:
        """Record stamps, mark the entries read-only, and refuse mutation after.

        `provenance` is an opaque per-path dict the caller supplies and this
        code never interprets -- fabfos passes the DVC pin each entry's id was
        derived from, which is what lets *it* tell a re-materialised pin (bytes
        identical, mtime moved) from a genuinely different one. Metasmith stays
        DVC-agnostic and round-trips the dict.

        `deep=True` additionally records a real content digest per entry --
        minutes to hours over 24 GB, and the only thing that makes
        `verify --deep` able to answer anything later. Without it, verification
        reports UNVERIFIABLE rather than OK, which is the honest answer.

        Re-running on an already-frozen library re-stamps it, which is the
        supported way to clear a false positive. It never re-mints an id.
        """
        provenance = provenance or {}
        entries: dict[str, dict] = {}
        missing: list[str] = []
        chmod_failed: list[str] = []
        for path in sorted(self.manifest, key=str):
            abs_path = self._abs(path)
            if not abs_path.exists():
                # Not an error: a library of remote paths, or one staged to a
                # host that has not materialised its data, is still worth
                # freezing for its ids. It simply has nothing to witness.
                missing.append(str(path))
                continue
            entry = _stamp(abs_path)
            if apply_permissions:
                try:
                    mode = abs_path.stat().st_mode
                    os.chmod(abs_path, mode & ~0o222)
                    entry["mode_applied"] = True
                except OSError as e:
                    # A shared HPC copy this user does not own. The freeze is
                    # still valid -- the mark was advisory anyway.
                    chmod_failed.append(f"{path}: {e}")
                    entry["mode_applied"] = False
            if deep:
                entry["content_digest"] = _content_digest(abs_path)
            prov = provenance.get(path)
            if prov is not None:
                entry["provenance"] = prov
            entries[str(path)] = entry
        self._frozen = {
            "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "host": socket.gethostname(),
            "entries": entries,
        }
        self._persist(update_types=True)
        for line in chmod_failed:
            Log.Warn(f"could not mark read-only -- {line}")
        return {
            "location": str(self.location),
            "frozen": len(entries),
            "missing": missing,
            "chmod_failed": chmod_failed,
        }

    def Unfreeze(self, *, restore_permissions: bool = True) -> dict:
        """Lift the freeze. Restores owner write on the entries it marked."""
        if not self.is_frozen:
            return {"location": str(self.location), "unfrozen": 0, "restored": []}
        entries = self._frozen.get("entries", {})
        restored: list[str] = []
        if restore_permissions:
            for name, entry in entries.items():
                if not entry.get("mode_applied"):
                    continue
                abs_path = self._abs(Path(name))
                if not abs_path.exists():
                    continue
                try:
                    os.chmod(abs_path, abs_path.stat().st_mode | 0o200)
                    restored.append(name)
                except OSError as e:
                    Log.Warn(f"could not restore write on [{name}]: {e}")
        self._frozen = None
        self._persist(update_types=True)
        return {
            "location": str(self.location),
            "unfrozen": len(entries),
            "restored": restored,
        }

    def Restamp(self, paths: Iterable[Path] | None = None) -> dict:
        """Re-record stamps without touching a single `instance_id`.

        The remedy for a false positive. A caller that knows the bytes are
        unchanged -- because the DVC pin it minted the ids from is unchanged --
        calls this and the ids stay exactly as they were.
        """
        if not self.is_frozen:
            raise FrozenLibraryError(
                f"[Restamp] refused: the library at [{self.location}] is not frozen"
            )
        targets = list(self.manifest) if paths is None else [Path(p) for p in paths]
        entries = self._frozen.setdefault("entries", {})
        before = {k: dict(v) for k, v in entries.items()}
        changed: dict[str, str] = {}
        for path in targets:
            abs_path = self._abs(path)
            if not abs_path.exists():
                continue
            key = str(path)
            old = entries.get(key, {})
            new = _stamp(abs_path)
            # `content_digest` is carried forward, NOT re-derived and NOT
            # dropped. Re-deriving would make a restamp quietly bless whatever
            # is on disk now, and dropping it would turn a DRIFTED verdict into
            # UNVERIFIABLE -- both launder "we did not check" into "it is fine",
            # which is the one thing this whole mechanism must not do. A restamp
            # asserts the STAMP moved; only `freeze --deep` may say anything
            # about the bytes.
            for carried in ("mode_applied", "provenance", "content_digest"):
                if carried in old:
                    new[carried] = old[carried]
            entries[key] = new
            drift = _describe({k: v for k, v in old.items() if k in _stamp_fields(new)},
                              {k: v for k, v in new.items() if k in _stamp_fields(new)})
            if old and drift != "no visible difference":
                changed[key] = drift
        self._frozen["at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        self._frozen["host"] = socket.gethostname()
        self._persist(update_types=True)
        return {"location": str(self.location), "restamped": len(targets), "changed": changed}

    def _verify_frozen_stamps(self) -> None:
        """Raise if a stamped entry visibly moved on the host that stamped it."""
        if not self.is_frozen:
            return
        if os.environ.get(_NOCHECK_ENV):
            Log.Warn(
                f"{_NOCHECK_ENV} is set -- serving the recorded ids of"
                f" [{self.location}] without checking them. This is an"
                " emergency override, not a fix; see Restamp()."
            )
            return
        recorded_host = self._frozen.get("host")
        here = socket.gethostname()
        drift: list[str] = []
        for name, entry in self._frozen.get("entries", {}).items():
            abs_path = self._abs(Path(name))
            if not abs_path.exists():
                # Existence is `check_integrity`'s job, and a frozen library
                # staged to an agent legitimately names paths this host does not
                # have. Nothing to compare.
                continue
            observed = _stamp(abs_path)
            recorded = {k: v for k, v in entry.items() if k in observed}
            if recorded == observed:
                continue
            drift.append(f"  [{name}] {_describe(recorded, observed)}")
        if not drift:
            return
        if recorded_host != here:
            Log.Warn(
                f"the frozen library at [{self.location}] was stamped on"
                f" [{recorded_host}] and this is [{here}]; mtime is not"
                " comparable across hosts, so the following are reported"
                " rather than refused:\n" + "\n".join(drift)
            )
            return
        raise FrozenLibraryError(
            f"the frozen library at [{self.location}] no longer matches what was"
            " recorded when it was frozen:\n" + "\n".join(drift) + "\n"
            "  The recorded instance_ids may no longer describe these bytes, and"
            " serving them would be a false cache hit.\n"
            "  If the bytes are unchanged and only the stamp moved (re-materialising"
            " the same pin does that), re-record it:\n"
            "    metasmith data restamp <library>\n"
            "  If the bytes did change, the ids are wrong and the library must be"
            " rebuilt and re-frozen."
        )

    def Verify(self, *, deep: bool = False) -> dict:
        """Report per entry, without raising. The escape hatch, on demand.

        `deep=True` re-derives a real content digest and compares it against
        what `Freeze(deep=True)` recorded. Every hole listed at the top of this
        file is closed by that comparison and by nothing else -- and where no
        baseline was recorded the verdict is UNVERIFIABLE, never OK. Reporting
        "we did not check" as "it is fine" is the one thing this tool must not
        do, since the whole reason to run it is a suspicion the cheap checks
        cannot settle.
        """
        if not self.is_frozen:
            return {"location": str(self.location), "frozen": False, "entries": {}}
        out: dict[str, dict] = {}
        for name, entry in sorted(self._frozen.get("entries", {}).items()):
            abs_path = self._abs(Path(name))
            row = {"kind": entry.get("kind")}
            if not abs_path.exists():
                row["verdict"] = "MISSING"
                out[name] = row
                continue
            observed = _stamp(abs_path)
            recorded = {k: v for k, v in entry.items() if k in observed}
            row["stamp"] = "OK" if recorded == observed else _describe(recorded, observed)
            if deep:
                baseline = entry.get("content_digest")
                if baseline is None:
                    row["verdict"] = "UNVERIFIABLE"
                    row["why"] = "no --deep baseline was recorded at freeze time"
                else:
                    now = _content_digest(abs_path)
                    row["verdict"] = "OK" if now == baseline else "DRIFTED"
                    if now != baseline:
                        row["content"] = f"recorded={baseline[:24]}… observed={(now or 'unreadable')[:24]}…"
            else:
                row["verdict"] = "OK" if recorded == observed else "DRIFTED"
            out[name] = row
        return {
            "location": str(self.location),
            "frozen": True,
            "host": self._frozen.get("host"),
            "at": self._frozen.get("at"),
            "deep": deep,
            "entries": out,
        }
