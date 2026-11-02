# A library whose recorded identities are taken at their word.
#
# A `DataInstanceLibrary` derives a leaf's `instance_id` from where the file is
# and when it last changed -- one stat, re-taken at staging time on the host that
# owns it. That is cheap enough for anything, and it is still the wrong identity
# for a set of reference databases: a `dvc checkout` restoring the very same
# bytes moves mtime, so every id under 24 GB of references would move and take
# every downstream cache shard with it.
#
# Pinning is how a library says its recorded ids are already correct. A pinned
# library refuses every mutation, returns `instance_meta` entries verbatim without
# consulting the filesystem, and records a cheap witness -- a stat stamp per
# top-level entry -- that `Load` checks so a library whose bytes visibly moved
# raises instead of silently serving an id that no longer describes them.
#
# **Nothing here touches file modes.** Protecting the bytes is the storage layer's
# job -- for the fabfos references, DVC's -- and marking entries read-only from
# here only ever bought accident-prevention the owner could undo, at the price of
# an EACCES that broke the next `dvc checkout`. The refusals bind callers of this
# API, not the filesystem, and that is the whole of the guarantee.
#
# ## What the stamp does NOT catch
#
# Read this before trusting it, and before changing it. It is a smoke alarm, not a
# lock. The failure direction is asymmetric and bad: an undetected content swap
# under an unchanged id is a false cache *hit*, which replays a stale shard and
# produces silently wrong scientific output with no error anywhere. That asymmetry
# is why this is written down rather than reassured about.
#
# **The stat stamp `(size, mtime_ns)` does not reach:**
#
# - *mtime is not content.* A same-size in-place edit that preserves mtime
#   (`cp -p`, `rsync --times`, `tar -p`, `touch -r`) passes undetected.
# - *Directories are the weak case, and they are the entries that most need it.* A
#   directory's mtime reflects only its own entry list and its inode size means
#   nothing, so a change nested inside one is invisible. The immediate-entry count
#   is recorded to make the stamp less vacuous; it is still weak.
# - *Across hosts.* mtime granularity and clock skew on the NFS/Lustre filesystems
#   the HPC copies live on make stamps non-comparable, so a stamp taken on another
#   host warns rather than raises. Honest coverage is "the machine that pinned it".
# - *Tamper evidence.* The stamp lives in the file it validates and re-pinning
#   silently re-stamps. This is a consistency check, not an integrity check.
#
# The likelier day-to-day failure is the **false positive**: re-materialising the
# same DVC pin moves mtime, and the bytes are fine. `Restamp()` is the remedy --
# it re-records stamps and moves no `instance_id`. Turning the check off is not
# the remedy, which is why there is a verb for this and the kill switch
# (`METASMITH_PINNED_NOCHECK=1`) is documented as an emergency, not a fix.
#
# **The escape hatch with none of these holes** is `metasmith data verify --deep`,
# which re-derives real content digests and compares them against what `pin
# --deep` recorded. Expensive, never automatic: run it before a release or after a
# cache hit you did not expect. Where no `--deep` baseline exists it reports
# `UNVERIFIABLE`, never `OK` -- the tool must not launder "we did not check" into
# "it is fine".
#
# ## Where a pinned id comes from
#
# Not from a stat. fabfos mints these from the DVC pin's md5, a digest over the
# real bytes that every host checking out the same pin agrees on -- host-portable
# where a stat-derived id is not, and stable across the re-materialisation that
# would move one. That is the whole reason a pin is worth keeping. The stamp
# below only raises a question about an id that already exists.

from __future__ import annotations

import os
import socket
import stat as stat_mod
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from ...logging import Log


class PinnedLibraryError(RuntimeError):
    # A refusal by a pinned library, or a stamp that no longer matches.
    pass


#: Emergency only. Documented in the module docstring as *not* the remedy for a
#: false positive -- `Restamp()` is. Present because a stamp that raises on a
#: cluster at 3am must be defeatable by someone who cannot edit the index.
_NOCHECK_ENV = "METASMITH_PINNED_NOCHECK"


def _stamp(abs_path: Path) -> dict:
    # A cheap witness that `abs_path` has not visibly moved.
    #
    # One `stat` for a file. For a directory, one `stat` plus one `listdir` of the
    # immediate entries -- see the module docstring for exactly how little that
    # proves.
    st = abs_path.stat()
    if stat_mod.S_ISDIR(st.st_mode):
        try:
            n = len(os.listdir(abs_path))
        except OSError:
            n = -1
        return {"kind": "dir", "mtime_ns": st.st_mtime_ns, "n_entries": n}
    return {"kind": "file", "size": st.st_size, "mtime_ns": st.st_mtime_ns}


def _content_digest(abs_path: Path, *, force: bool = False) -> str | None:
    # A real digest of the bytes. Expensive; only `--deep` asks for it.
    #
    # `force` bypasses the per-process file-digest memo. A deep verify in a warm
    # process is asking whether the bytes moved; served from a memo keyed on
    # `(path, size, mtime_ns)` it would answer with the digest of the bytes that
    # were there when the memo was filled, which is the one answer it must not give.
    from ...caching.keys import content_multihash_key, tree_multihash_key

    try:
        if abs_path.is_dir():
            return tree_multihash_key(abs_path, force=force).hex()
        return content_multihash_key(abs_path).hex()
    except OSError:
        return None


def _stamp_fields(entry: dict) -> set[str]:
    # Just the witness, not the bookkeeping recorded beside it.
    return {"kind", "size", "mtime_ns", "n_entries"} & set(entry)


def _describe(recorded: dict, observed: dict) -> str:
    keys = sorted(set(recorded) | set(observed))
    parts = [
        f"{k}: recorded={recorded.get(k)!r} observed={observed.get(k)!r}"
        for k in keys
        if recorded.get(k) != observed.get(k)
    ]
    return "; ".join(parts) or "no visible difference"


class _PinnedLibrary:
    #: The raw `pinned:` block from `index.yml`, or None. Absent means not
    #: pinned, which is why every library written before this existed keeps
    #: loading unchanged.
    _pinned: dict | None = None

    @property
    def is_pinned(self) -> bool:
        return self._pinned is not None

    def _refuse_if_pinned(self, verb: str) -> None:
        if not self.is_pinned:
            return
        raise PinnedLibraryError(
            f"[{verb}] refused: the library at [{self.location}] is pinned."
            " Its recorded instance_ids are what downstream cache keys are built"
            " from, so changing it here would re-key every run that used it."
            " Unpin deliberately (`metasmith data unpin`) if that is what"
            " you mean."
        )

    def _abs(self, path: Path) -> Path:
        return path if path.is_absolute() else self.location / path

    def Pin(
        self,
        *,
        provenance: dict[Path, dict] | None = None,
        deep: bool = False,
    ) -> dict:
        # Record stamps and refuse mutation after.
        #
        # `provenance` is an opaque per-path dict the caller supplies and this
        # code never interprets -- fabfos passes the DVC pin each entry's id was
        # derived from, which is what lets *it* tell a re-materialised pin (bytes
        # identical, mtime moved) from a genuinely different one. Metasmith stays
        # DVC-agnostic and round-trips the dict.
        #
        # `deep=True` additionally records a real content digest per entry --
        # minutes to hours over 24 GB, and the only thing that makes
        # `verify --deep` able to answer anything later. Without it, verification
        # reports UNVERIFIABLE rather than OK, which is the honest answer.
        #
        # Re-running on an already-pinned library re-stamps it, which is the
        # supported way to clear a false positive. It never re-mints an id.
        provenance = provenance or {}
        entries: dict[str, dict] = {}
        missing: list[str] = []
        for path in sorted(self.manifest, key=str):
            abs_path = self._abs(path)
            if not abs_path.exists():
                # Not an error: a library of remote paths, or one staged to a
                # host that has not materialised its data, is still worth
                # pinning for its ids. It simply has nothing to witness.
                missing.append(str(path))
                continue
            entry = _stamp(abs_path)
            if deep:
                entry["content_digest"] = _content_digest(abs_path, force=True)
            prov = provenance.get(path)
            if prov is not None:
                entry["provenance"] = prov
            entries[str(path)] = entry
        self._pinned = {
            "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "host": socket.gethostname(),
            "entries": entries,
        }
        self._persist(update_types=True)
        return {
            "location": str(self.location),
            "pinned": len(entries),
            "missing": missing,
        }

    def Unpin(self) -> dict:
        # Lift the pin, so the library can be rebuilt and re-pinned.
        if not self.is_pinned:
            return {"location": str(self.location), "unpinned": 0}
        entries = self._pinned.get("entries", {})
        self._pinned = None
        self._persist(update_types=True)
        return {"location": str(self.location), "unpinned": len(entries)}

    def Restamp(self, paths: Iterable[Path] | None = None) -> dict:
        # Re-record stamps without touching a single `instance_id`.
        #
        # The remedy for a false positive. A caller that knows the bytes are
        # unchanged -- because the DVC pin it minted the ids from is unchanged --
        # calls this and the ids stay exactly as they were.
        if not self.is_pinned:
            raise PinnedLibraryError(
                f"[Restamp] refused: the library at [{self.location}] is not pinned"
            )
        targets = list(self.manifest) if paths is None else [Path(p) for p in paths]
        entries = self._pinned.setdefault("entries", {})
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
            # asserts the STAMP moved; only `pin --deep` may say anything
            # about the bytes.
            for carried in ("provenance", "content_digest"):
                if carried in old:
                    new[carried] = old[carried]
            entries[key] = new
            drift = _describe({k: v for k, v in old.items() if k in _stamp_fields(new)},
                              {k: v for k, v in new.items() if k in _stamp_fields(new)})
            if old and drift != "no visible difference":
                changed[key] = drift
        self._pinned["at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        self._pinned["host"] = socket.gethostname()
        self._persist(update_types=True)
        return {"location": str(self.location), "restamped": len(targets), "changed": changed}

    def _verify_pinned_stamps(self) -> None:
        # Raise if a stamped entry visibly moved on the host that stamped it.
        if not self.is_pinned:
            return
        if os.environ.get(_NOCHECK_ENV):
            Log.Warn(
                f"{_NOCHECK_ENV} is set -- serving the recorded ids of"
                f" [{self.location}] without checking them. This is an"
                " emergency override, not a fix; see Restamp()."
            )
            return
        recorded_host = self._pinned.get("host")
        here = socket.gethostname()
        drift: list[str] = []
        for name, entry in self._pinned.get("entries", {}).items():
            abs_path = self._abs(Path(name))
            if not abs_path.exists():
                # Existence is `check_integrity`'s job, and a pinned library
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
                f"the pinned library at [{self.location}] was stamped on"
                f" [{recorded_host}] and this is [{here}]; mtime is not"
                " comparable across hosts, so the following are reported"
                " rather than refused:\n" + "\n".join(drift)
            )
            return
        raise PinnedLibraryError(
            f"the pinned library at [{self.location}] no longer matches what was"
            " recorded when it was pinned:\n" + "\n".join(drift) + "\n"
            "  The recorded instance_ids may no longer describe these bytes, and"
            " serving them would be a false cache hit.\n"
            "  If the bytes are unchanged and only the stamp moved (re-materialising"
            " the same pin does that), re-record it:\n"
            "    metasmith data restamp <library>\n"
            "  If the bytes did change, the ids are wrong and the library must be"
            " rebuilt and re-pinned."
        )

    def Verify(self, *, deep: bool = False) -> dict:
        # Report per entry, without raising. The escape hatch, on demand.
        #
        # `deep=True` re-derives a real content digest and compares it against
        # what `Pin(deep=True)` recorded. Every hole listed at the top of this
        # file is closed by that comparison and by nothing else -- and where no
        # baseline was recorded the verdict is UNVERIFIABLE, never OK. Reporting
        # "we did not check" as "it is fine" is the one thing this tool must not
        # do, since the whole reason to run it is a suspicion the cheap checks
        # cannot settle.
        if not self.is_pinned:
            return {"location": str(self.location), "pinned": False, "entries": {}}
        out: dict[str, dict] = {}
        for name, entry in sorted(self._pinned.get("entries", {}).items()):
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
                    row["why"] = "no --deep baseline was recorded at pin time"
                else:
                    now = _content_digest(abs_path, force=True)
                    row["verdict"] = "OK" if now == baseline else "DRIFTED"
                    if now != baseline:
                        row["content"] = f"recorded={baseline[:24]}… observed={(now or 'unreadable')[:24]}…"
            else:
                row["verdict"] = "OK" if recorded == observed else "DRIFTED"
            out[name] = row
        return {
            "location": str(self.location),
            "pinned": True,
            "host": self._pinned.get("host"),
            "at": self._pinned.get("at"),
            "deep": deep,
            "entries": out,
        }
