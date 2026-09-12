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

    def PoolGivens(self) -> "PoolGivens":
        """A collector for the givens a driver is about to declare."""
        return PoolGivens(self)

    def _pool_root(self) -> Path:
        from ..caching.layout import default_cache_root

        return default_cache_root(Path(self.home.GetPath()))

    def _pool_command(self, verb: str, flags: list[str]) -> str:
        # The setup commands come first for the same reason `_run_setup` runs
        # them: a cluster reaches its container runtime through a module load,
        # and a non-login ssh command inherits none of that.
        home = shlex.quote(str(self.home.GetPath()))
        parts = list(self.setup_commands)
        parts.append(f"cd {home} && ./msm --json {verb} " + " ".join(flags))
        return " ; ".join(parts)

    def _pool_read_command(self, flags: list[str]) -> str:
        return self._pool_command("cache list", flags)

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

    def ImportToPool(
        self,
        path,
        dtype: str,
        *,
        name: str | None = None,
        parents=None,
        tags=None,
        timeout: int = 300,
    ) -> dict:
        """Record one item in the agent's pool, where that item already sits.

        The write half of the read path. Nothing is copied, moved or read, and
        the path is the agent's own -- so this runs on the agent for the same
        reason `ReadPool` does, and for a local home it is the ordinary call.

        A setup act, not a driver's. Every call is a separate import and gets
        its own identity, so calling it twice for one thing is how a caller
        says the second declaration is a second thing. `EnsurePoolEntries` is
        the form to reach for when that is not what is meant.
        """
        from ..ops import data as op_data

        parents = list(parents or [])
        tags = list(tags or [])
        if not self._is_ssh():
            return op_data.import_item(
                str(path), dtype,
                agent_home=str(self.home.GetPath()),
                name=name, parents=parents, tags=tags,
            )
        flags = [
            shlex.quote(str(path)),
            f"--dtype {shlex.quote(dtype)}",
            f"--cache-root {shlex.quote(str(self._pool_root()))}",
        ]
        if name is not None:
            flags.append(f"--name {shlex.quote(name)}")
        for p in parents:
            flags.append(f"--parent {shlex.quote(str(p))}")
        for t in tags:
            flags.append(f"--tag {shlex.quote(t)}")
        res = self._remote_oneshot(
            self._pool_command("data import", flags), timeout=timeout,
        )
        try:
            return first_json(res.out)
        except ValueError as e:
            raise ValueError(
                f"could not import [{path}] into the pool at "
                f"[{self._pool_root()}] on [{self.home.address}]: {e}"
            ) from None

    def WriteImportable(self, relpath, content: str, *, timeout: int = 120) -> Path:
        """Put a small file this process authored where the pool can name it.

        A pool records where data sits and never holds a copy, so a file a
        driver writes has to exist on the agent's filesystem before it can be
        imported. A path under the client's own cache directory is not a thing
        the pool can point at.

        Only for the small things a driver authors -- a metadata document, a
        policy marker. Anything with bytes worth moving is moved by whatever
        put it on that host in the first place.

        The write is skipped when the file already says this, so calling it
        again is a comparison rather than a change.
        """
        import base64

        target = Path(self.home.GetPath()) / "imports" / Path(relpath)
        if not self._is_ssh():
            target.parent.mkdir(parents=True, exist_ok=True)
            if not target.exists() or target.read_text() != content:
                target.write_text(content)
            return target

        # base64 rather than a quoted heredoc: the content is arbitrary and the
        # command crosses a shell, a login profile and possibly a container.
        blob = base64.b64encode(content.encode("utf-8")).decode("ascii")
        tmp = f"{target}.tmp.$$"
        script = (
            f"mkdir -p {shlex.quote(str(target.parent))} && "
            f"printf %s {shlex.quote(blob)} | base64 -d > {shlex.quote(tmp)} && "
            f"(cmp -s {shlex.quote(tmp)} {shlex.quote(str(target))} "
            f"&& rm {shlex.quote(tmp)} "
            f"|| mv {shlex.quote(tmp)} {shlex.quote(str(target))})"
        )
        parts = list(self.setup_commands) + [script]
        self._remote_oneshot(" ; ".join(parts), timeout=timeout)
        return target

    def EnsurePoolEntries(self, items, *, timeout: int = 300) -> dict:
        """Make sure the pool holds an entry under each name, and say which.

        What a driver's setup runs. An item the pool already holds under the
        name given is left alone and referenced, so running setup again is a
        read rather than a second import -- which is what keeps a plan built
        today keying the same as the one built last week.

        Each item is a mapping of `path` and `dtype`, plus an optional `name`
        (defaulting to the path), `parents` and `tags`. Parents are resolved
        against what the pool holds, so an item may name one imported earlier
        in the same list by its name.
        """
        held: dict[str, str] = {}
        for row in self.ReadPool(origin="imported")["entries"]:
            held.setdefault(row["name"], row["instance_id"])

        out: dict[str, str] = {}
        for item in items:
            name = str(item.get("name") or item["path"])
            if name in held:
                out[name] = held[name]
                continue
            parents = [out.get(str(p), p) for p in item.get("parents") or []]
            record = self.ImportToPool(
                item["path"], item["dtype"],
                name=name, parents=parents, tags=item.get("tags"),
                timeout=timeout,
            )
            held[name] = out[name] = record["instance_id"]
        return out

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


class PoolGivens:
    """What a driver declares, collected, then made into a given library.

    Two acts that used to be one. `AddItem` both declared what a file was and
    minted an identity for it on the spot, from a filesystem the driver often
    could not see. Declaring and identifying are now separated: a driver says
    what it has and what to call it, the pool assigns the identity once, and
    every run after that cites the name.

    `Add` returns the name rather than a path, and a name is what `parents`
    takes -- so the shape of a driver's input-building code survives the move
    with the same variables threading the same edges.
    """

    def __init__(self, agent):
        self.agent = agent
        self.items: list[dict] = []

    def Add(self, path, dtype: str, *, name: str | None = None,
            parents=(), tags=()) -> str:
        """Declare a file or folder that already sits on the agent's host."""
        name = name or str(path)
        self.items.append({
            "path": path, "dtype": dtype, "name": name,
            "parents": list(parents), "tags": list(tags),
        })
        return name

    def Value(self, name: str, content, dtype: str, *, parents=(), tags=()) -> str:
        """Declare a small document this process authors, written on the agent.

        The counterpart of `AddValue`, minus its defect: that one wrote the
        file and then read back the mtime it had just created, so identical
        content was handed a new identity on every call.
        """
        import json as _json

        if not isinstance(content, str):
            content = _json.dumps(content, sort_keys=True)
        target = self.agent.WriteImportable(name, content)
        return self.Add(target, dtype, name=name, parents=parents, tags=tags)

    def Build(self, location, *, types=None, type_library_paths=None,
              ensure: bool = True, timeout: int = 300):
        """Cite what was declared, importing first whatever the pool lacks.

        `ensure=False` cites only, so a driver that is not doing setup fails
        by name on anything nobody imported, rather than importing it as a
        side effect of a run.
        """
        if ensure:
            self.agent.EnsurePoolEntries(self.items, timeout=timeout)
        return self.agent.GivenLibrary(
            [i["name"] for i in self.items],
            location=location,
            types=types,
            type_library_paths=type_library_paths,
        )
