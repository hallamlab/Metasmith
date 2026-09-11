"""A client's read of an agent's pool, wherever that agent's home is.

The pool a plan's givens come from lives in the agent home, which is usually a
cluster the client cannot reach into. It does not have to: a given is a name, a
type and an identity, and all three are records the pool already keeps. So this
asks the agent's own metasmith what it holds and reads the answer, rather than
fetching anything.

**Read, never materialize.** A resolved reference carries a path, and that path
is meaningful on the agent's filesystem alone. Nothing here opens one, and a
caller that tries is asking for the store-staging job this deliberately is not.
The client never needs the bytes, because the identity is a record rather than
a derivation from them -- which is the whole reason an import assigns one.

The transport is one ssh command, the same as every other remote read on the
agent. `./msm` is the wrapper the deploy writes, so the pool is read by the
engine that owns it and this end parses JSON instead of a database file.
"""

from __future__ import annotations

import json
import shlex
from pathlib import Path


_UNKNOWN = (
    "[{ref}] is not in the pool at [{root}]. Import it on the agent first:\n"
    "  metasmith data import <path> --dtype <NS::TYPE> --name {ref} "
    "--agent-home <home>\n"
    "An identity is assigned by the import, so there is nothing this end can "
    "mint for a name the pool has never seen."
)


def first_json(lines) -> dict:
    """The first complete JSON value in a stream that also carries noise.

    The agent's `msm` wrapper echoes its bind list before it runs anything, and
    a container runtime prints whatever it likes around that. Metasmith's own
    `--json` output is clean -- `_main` moves the log stream to stderr -- so the
    answer is the first well-formed value, and `raw_decode` stops at its end
    rather than choking on what follows.
    """
    text = "\n".join(lines)
    for start, ch in enumerate(text):
        if ch not in "{[":
            continue
        try:
            value, _end = json.JSONDecoder().raw_decode(text, start)
        except ValueError:
            continue
        return value
    raise ValueError(
        f"the agent returned no JSON. What it did return:\n" + "\n".join(lines[-20:])
    )


class _PoolAccess:

    def _pool_root(self) -> Path:
        from ..caching.layout import default_cache_root

        return default_cache_root(Path(self.home.GetPath()))

    def _pool_read_command(self, flags: list[str]) -> str:
        # The setup commands come first for the same reason `_run_setup` runs
        # them: a cluster reaches its container runtime through a module load,
        # and a non-login ssh command inherits none of that.
        home = shlex.quote(str(self.home.GetPath()))
        parts = list(self.setup_commands)
        parts.append(
            f"cd {home} && ./msm --json cache list "
            + " ".join(flags)
        )
        return " ; ".join(parts)

    def ReadPool(
        self,
        *,
        origin: str | None = None,
        dtype: str | None = None,
        tag: str | None = None,
        run: str | None = None,
        name: str | None = None,
        timeout: int = 120,
    ) -> dict:
        """What the agent's pool holds, as the agent itself reports it."""
        from ..ops import cache as op_cache

        where = dict(origin=origin, dtype=dtype, tag=tag, run=run, name=name)
        if not self._is_ssh():
            return op_cache.list_cache(
                agent_home=str(self.home.GetPath()), **where,
            )
        flags = [f"--cache-root {shlex.quote(str(self._pool_root()))}"]
        for k, v in where.items():
            if v is not None:
                flags.append(f"--{k} {shlex.quote(str(v))}")
        res = self._remote_oneshot(self._pool_read_command(flags), timeout=timeout)
        try:
            return first_json(res.out)
        except ValueError as e:
            raise ValueError(
                f"could not read the pool at [{self._pool_root()}] on "
                f"[{self.home.address}]: {e}"
            ) from None

    def ResolvePoolRefs(self, refs, *, entries: list | None = None) -> list[dict]:
        """Pool entries for the references given, in the order given.

        A reference is the name an import recorded, or an instance id. A name
        that matches nothing is refused by naming the import call, and one that
        matches several entries is refused by listing their ids -- re-importing
        a name is how a caller says this is a new thing, so the pool holding two
        of them is expected and choosing between them is not this end's call.
        """
        rows = entries if entries is not None else self.ReadPool()["entries"]
        by_name: dict[str, list[dict]] = {}
        by_id: dict[str, dict] = {}
        for r in rows:
            if r.get("name"):
                by_name.setdefault(r["name"], []).append(r)
            if r.get("instance_id"):
                by_id[r["instance_id"]] = r

        out = []
        for ref in refs:
            ref = str(ref)
            if ref in by_id:
                out.append(by_id[ref])
                continue
            hits = by_name.get(ref, [])
            if len(hits) == 1:
                out.append(hits[0])
                continue
            if not hits:
                raise ValueError(
                    _UNKNOWN.format(ref=ref, root=self._pool_root())
                )
            ids = ", ".join(sorted(h["instance_id"] for h in hits))
            raise ValueError(
                f"[{ref}] names [{len(hits)}] entries in the pool at "
                f"[{self._pool_root()}]. Each import assigned its own identity, "
                f"so name the one you mean by id: {ids}"
            )
        return out

    def GivenLibrary(
        self,
        refs,
        *,
        location,
        types: dict | None = None,
        type_library_paths: list | None = None,
        entries: list | None = None,
    ):
        """A given library holding exactly the pool entries named, in order.

        The instances carry the pool's identities, so a plan built twice from
        the same references keys the same both times. That is the whole point:
        an identity assigned at import cannot move, where one derived from a
        path and an mtime moved on every submission and took the run directory
        with it.

        Nothing here reads the data. The paths are the agent's, the types come
        from the libraries the caller attaches, and the ancestry is the edges
        the pool recorded -- an edge to an entry not among the references is
        dropped, because a parent the library has no item for is a path
        `Unpack` would walk into and not find.
        """
        from ..models.libraries import DataInstanceLibrary, DataTypeLibrary

        dtypes: dict = dict(types or {})
        for p in type_library_paths or []:
            p = Path(p)
            dtypes.setdefault(p.stem, DataTypeLibrary.Load(p))

        rows = self.ResolvePoolRefs(refs, entries=entries)
        dtype_by_id = {r["instance_id"]: r["dtype"] for r in rows}
        path_by_id = {r["instance_id"]: r["path"] for r in rows}

        manifest: dict = {}
        for r in rows:
            packed = {
                "type": r["dtype"],
                "instance_id": r["instance_id"],
                "origin": r.get("origin", "imported"),
            }
            if r.get("lineage_payload"):
                packed["lineage_payload"] = r["lineage_payload"]
            parents = {
                f"pool@{path_by_id[pid]}": dtype_by_id[pid]
                for pid in r.get("parents", []) if pid in path_by_id
            }
            if parents:
                packed["parents"] = parents
            manifest[r["path"]] = packed

        lib = DataInstanceLibrary.Unpack(
            location=Path(location),
            raw={"schema": DataInstanceLibrary.schema, "manifest": manifest},
            dtypes=dtypes,
        )
        lib.types = dtypes
        return lib
