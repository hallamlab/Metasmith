from __future__ import annotations

import os
import shutil
from pathlib import Path

from ...logging import Log
from ..remote import Logistics, Source
from ._atomic import write_yaml_atomic
from .types import DataTypeLibrary, yaml_safe_load


class _StoreTransfer:
    def Pack(self):
        def _pack_instance(path, dtype_name):
            grandparents = set()
            for p in self.parents.get(path, []):
                for gp in self.parents.get(p.path, []):
                    grandparents.add(gp.path)
            d_parents = {}
            for p in self.parents.get(path, []):
                if p.path in grandparents: continue
                p.library_key
                k = f"{p.library_key}@{p.path}"
                v = p.name
                d_parents[k] = v
            d = dict(
                type=dtype_name,
            )
            if len(d_parents) > 0:
                d["parents"] = dict(sorted(d_parents.items(), key=lambda t:t[0]))
            meta = self.instance_meta.get(path)
            if meta is not None:
                d["instance_id"] = meta["instance_id"]
                d["origin"] = meta.get("origin", "leaf")
                payload = meta.get("lineage_payload")
                if payload is not None:
                    d["lineage_payload"] = payload.hex()
            return d
        if not self.is_pinned:
            for _path, _dtype in self.manifest.items():
                _meta = self.instance_meta.get(_path)
                if _meta is not None and _meta.get("fork_id") != self.fork_id:
                    self._resolve_instance_meta(_path, _dtype)
        man = {str(k):_pack_instance(k, v) for k, v in self.manifest.items()}
        man = dict(sorted(man.items(), key=lambda t: t[0]))
        packed = dict(
            schema=self.schema,
            manifest=man,
            fork_id=self.fork_id,
            remote_src=self.remote_src.Pack() if self.remote_src is not None else None,
            pinned=self._pinned,
        )
        return {k:v for k, v in packed.items() if v is not None}

    @classmethod
    def Unpack(cls, location: Path, raw: dict, dtypes: dict[str, DataTypeLibrary], check_integrity: bool=False):
        if "manifest" not in raw:
            raise ValueError(
                f"library index at [{location/cls._path_to_meta/(cls._index_name+cls._metadata_ext)}] "
                f"is malformed: missing 'manifest' key. "
                f"Was this directory compiled with `metasmith build`?"
            )
        manifest = {}
        instance_meta: dict[Path, dict] = {}
        for k, v in raw["manifest"].items():
            type_name = v["type"]
            if check_integrity:
                assert (location/k).exists(), f"[{k}], does not exist"
            cls._get_type(type_name, dtypes)
            manifest[Path(k)] = type_name
            if "instance_id" in v:
                payload = v.get("lineage_payload")
                if isinstance(payload, str):
                    payload = bytes.fromhex(payload)
                instance_meta[Path(k)] = {
                    "instance_id": v["instance_id"],
                    "origin": v.get("origin", "leaf"),
                    "lineage_payload": payload,
                    # The library's fork was never round-tripped onto its
                    # entries, so every leaf of a forked library came back
                    # looking stale and was re-minted on the first Get() after
                    # a Load. Pack() re-derives stale entries against the
                    # current fork before writing, so the ids on disk ARE the
                    # forked ids and stamping the fork back on is idempotent.
                    "fork_id": raw.get("fork_id"),
                }
        lib = cls(
            location=location,
        )
        lib.schema = raw["schema"]
        lib.manifest = manifest
        lib.instance_meta = instance_meta
        lib.fork_id = raw.get("fork_id")
        # `frozen` is what this block was called before the rename. Read, never
        # written: an index staged to a host that cannot re-pin it would
        # otherwise load unpinned and re-hash 24 GB per plan, silently.
        lib._pinned = raw.get("pinned", raw.get("frozen"))
        remote_src = raw.get("remote_src")
        lib.remote_src = Source.Unpack(remote_src) if remote_src is not None else None
        for k, v in raw["manifest"].items():
            parents: dict[Path, cls.ParentMetadata] = {}
            for p_key, p_name in v.get("parents", {}).items():
                lib_key, p_path_str = p_key.split("@", maxsplit=1)
                p_path = Path(p_path_str)
                namespace, dtype_name = p_name.split("::")
                _lib = dtypes[namespace]
                dtype = _lib.types[dtype_name]
                parents[p_path] = cls.ParentMetadata(
                    dtype=dtype,
                    name=p_name,
                    library_key=lib_key,
                    path=p_path,
                )
            if len(parents) > 0:
                lib.parents[Path(k)] = list(parents.values())

        ancestor_cache: dict[Path, dict[Path, cls.ParentMetadata]] = {}

        def _get_all_ancestors(k_path: Path) -> dict[Path, cls.ParentMetadata]:
            if k_path in ancestor_cache:
                return ancestor_cache[k_path]
            ancestors: dict[Path, cls.ParentMetadata] = {}
            for p in lib.parents.get(k_path, []):
                ancestors[p.path] = p
                for gp_path, gp in _get_all_ancestors(p.path).items():
                    if gp_path not in ancestors:
                        ancestors[gp_path] = gp
            ancestor_cache[k_path] = ancestors
            return ancestors

        for k in raw["manifest"].keys():
            k_path = Path(k)
            if k_path not in lib.parents:
                continue
            lib.parents[k_path] = list(_get_all_ancestors(k_path).values())

        return lib

    def Save(self, update_types=True):
        self._refuse_if_pinned("Save")
        self._persist(update_types=update_types)

    def _persist(self, update_types=True):
        ext = self._metadata_ext
        types_path = self.location/self._path_to_types
        types_path.mkdir(parents=True, exist_ok=True)
        for namespace, types_lib in self.types.items():
            local_path = types_path/(namespace+ext)
            if not update_types and local_path.exists(): continue
            types_lib.Save(local_path)

        metadata_path = self.location/self._path_to_meta
        metadata_path.mkdir(parents=True, exist_ok=True)
        index_path = metadata_path/(self._index_name+ext)
        index_path.parent.mkdir(parents=True, exist_ok=True)
        write_yaml_atomic(index_path, self.Pack())

    def _ensure_saved(self, update_types=True):
        if self.is_pinned:
            return
        self._persist(update_types=update_types)

    @classmethod
    def Load(cls, path: Path|str, check_integrity=False, attach_trace: bool=True,
             check_pinned_stamps: bool=True):
        path = Path(path)
        ext = cls._metadata_ext
        meta_path = path/cls._path_to_meta
        types_path = path/cls._path_to_types
        index_path = meta_path/(cls._index_name+ext)
        assert path.exists(), f"path [{path}] does not exist"
        if not index_path.exists():
            uncompiled = any(path.glob("*.yml")) or any(path.glob("*.py"))
            hint = (
                " -- this looks like an uncompiled library: it holds sources but"
                " no compiled _metadata/. Run `metasmith build` against it first."
            ) if uncompiled else ""
            raise AssertionError(f"index file [{index_path}] does not exist{hint}")

        dtypes = {}
        for p in types_path.iterdir():
            if p.is_dir(): continue
            if p == index_path: continue
            k = p.relative_to(types_path).with_suffix("")
            k = str(k)
            dtypes[k] = DataTypeLibrary.Load(p)

        d = yaml_safe_load(index_path)
        self = cls.Unpack(location=path, raw=d, dtypes=dtypes, check_integrity=check_integrity)
        self.types = dtypes
        self._calculate_key(_raw_override=d)
        if check_pinned_stamps:
            self._verify_pinned_stamps()
        if attach_trace:
            for candidate in (
                path / "_metasmith" / "trace.jsonl",
                path.parent / "_metasmith" / "trace.jsonl",
            ):
                if candidate.exists():
                    try:
                        self.attach_trace(candidate)
                    except Exception as e:
                        Log.Warn(f"trace.jsonl at {candidate} failed to attach: {e}")
                    break
        return self

    def PackInline(self, root: Path) -> dict:
        root = Path(root).resolve()
        def relpath(p: Path) -> str:
            try:
                return str(Path(p).resolve().relative_to(root))
            except ValueError:
                return str(p)
        missing = sorted(ns for ns in self.types if ns not in self._type_sources)
        assert not missing, (
            f"can not inline type namespace(s) {missing}: added from something "
            f"other than a plain path, so there is no source to reference"
        )
        packed = self.Pack()
        packed["types"] = {ns: relpath(p) for ns, p in self._type_sources.items()}
        values = {}
        for path in self.manifest:
            fp = self.location / path
            if fp.is_file():
                values[str(path)] = fp.read_text()
        if values:
            packed["values"] = values
        return packed

    @classmethod
    def FromInline(cls, raw: dict, location: Path):
        dtypes = {ns: DataTypeLibrary.Load(Path(p)) for ns, p in raw.get("types", {}).items()}
        lib = cls.Unpack(location=Path(location), raw=raw, dtypes=dtypes)
        lib.types = dtypes
        for rel, content in raw.get("values", {}).items():
            fp = lib.location / rel
            fp.parent.mkdir(parents=True, exist_ok=True)
            fp.write_text(content)
        return lib

    def PrepTransfer(self, dest: Source, mover: Logistics|None=None):
        self._ensure_saved()
        for p, name, dtype in self.Iterate():
            assert p.is_absolute() or (self.location/p).exists(), f"file not found [{p}]"
        if mover is None:
            mover = Logistics()
        mover.QueueTransfer(
            src=Source.FromLocal(self.location),
            dest=dest,
        )
        return mover

    _IMAGE_SKIP_DIRS = {"__pycache__"}

    def MaterializeImage(self, dest: Path, drop: set[Path]) -> int:
        # A copy of the library tree MINUS the named manifest entries. Stated as
        # a subtraction rather than a selection because the manifest does not
        # list everything a transform needs at runtime: `build_libraries` skips
        # `_`-prefixed files, and several of those are helper scripts their
        # neighbours copy out by `Path(__file__).parent`. Selecting from the
        # manifest would drop them with nothing to say so.
        dest = Path(dest)
        src_root = self.location
        drop = {Path(p) for p in drop if not Path(p).is_absolute()}

        def _dropped(rel: Path) -> bool:
            return any(rel == d or d in rel.parents for d in drop)

        meta_root = Path(self._path_to_meta)

        def _reproduce(src: Path, dst: Path, rel: Path):
            dst.parent.mkdir(parents=True, exist_ok=True)
            if dst.is_symlink() or dst.exists():
                dst.unlink()
            if src.is_symlink():
                # Staging runs with resolve_symlinks=False, so a library that
                # went through Consolidate() ships links whose targets resolve
                # on the remote. Resolving them here would send the bytes.
                os.symlink(os.readlink(src), dst)
                return
            # The image's own metadata gets rewritten by the prune that follows,
            # so it is copied. Everything else is content the image only reads.
            if meta_root not in rel.parents:
                try:
                    os.link(src, dst)
                    return
                except OSError:
                    pass
            shutil.copy2(src, dst)

        dest.mkdir(parents=True, exist_ok=True)
        made: list[Path] = []
        for here, dirs, files in os.walk(src_root, followlinks=False):
            here = Path(here)
            rel_dir = here.relative_to(src_root)
            linked_dirs = [d for d in dirs if (here/d).is_symlink()]
            dirs[:] = [
                d for d in dirs
                if d not in self._IMAGE_SKIP_DIRS
                and d not in linked_dirs
                and not _dropped(rel_dir/d)
            ]
            (dest/rel_dir).mkdir(parents=True, exist_ok=True)
            made.append(rel_dir)
            for name in files + linked_dirs:
                if name.endswith(".pyc"): continue
                rel = rel_dir/name
                if _dropped(rel): continue
                _reproduce(here/name, dest/rel, rel)
        # Deepest first, after every child is in place: writing a child bumps
        # its parent's mtime, and a pinned library's witness for a directory
        # entry is that mtime.
        for rel_dir in sorted(made, key=lambda p: len(p.parts), reverse=True):
            shutil.copystat(src_root/rel_dir, dest/rel_dir)
        return sum(1 for d in drop if (src_root/d).exists())

    def SaveAs(self, dest: Source, label: str|None=None):
        mover = self.PrepTransfer(dest)
        res = mover.ExecuteTransfers(label=label)
        assert len(res.completed) == 1, f"move failed"
        return res

    @classmethod
    def LoadFrom(cls, src: Source, dest: Path|str, as_image=True, on_exist: str = "skip", label: str|None=None, resolve_symlinks: bool=True):
        assert isinstance(src, Source)
        assert on_exist in {"skip", "error", "clear", "update"}
        if not isinstance(dest, Path):
            dest = Path(dest)

        def _transfer():
            mover = Logistics()
            if as_image:
                _src = src/cls._path_to_meta
                _dest = dest/cls._path_to_meta
            else:
                _src, _dest = src, dest
            mover.QueueTransfer(
                src=_src,
                dest=Source.FromLocal(_dest),
            )
            res = mover.ExecuteTransfers(label=label, resolve_symlinks=resolve_symlinks)
            assert len(res.completed) == 1, f"move failed"
        if dest.exists():
            if on_exist == "error":
                raise FileExistsError(f"[{dest}] already exists")
            elif on_exist == "update":
                _transfer()
            elif on_exist == "clear":
                Log.Warn("clearing previously loaded library")
                shutil.rmtree(dest)
                _transfer()
            elif on_exist == "skip":
                pass
        else:
            _transfer()

        lib = cls.Load(dest, check_integrity=False)
        if as_image:
            lib.remote_src = src
            lib._refuse_if_pinned("LoadFrom(as_image=True)")
            lib._ensure_saved()
        return lib

    def Consolidate(self):
        digits = len(f"{len(self.manifest)}")
        new_paths: dict[Path, Path] = {}
        for i, p in enumerate(self.manifest):
            if not p.is_absolute(): continue
            local_link = Path(f"{i+1:0{digits}}_{p.name}")
            new_paths[p] = local_link
            lp = self.location/local_link
            if lp.exists(): continue
            lp.symlink_to(p, p.is_dir())
        return new_paths

    def ActualizeRemote(self, extern_dest: Source|None=None, label: str|None=None):
        if self.remote_src is None:
            return self
        _lib = None
        try:
            _lib = self.Load(self.location, check_integrity=True)
            return _lib
        except AssertionError:
            pass
        if _lib is None:
            mover = Logistics()
            if extern_dest is None:
                extern_dest = Source.FromLocal(self.location)
            mover.QueueTransfer(
                src=self.remote_src,
                dest=extern_dest,
            )
            res = mover.ExecuteTransfers(label=label)
            assert len(res.completed) == 1, f"failed to load library from [{self.remote_src}]; [{res.errors}]"
        _lib = self.Load(self.location, check_integrity=True)
        return _lib
    
    def LocalizeContents(self):
        # Copies absolute entries INTO the library and rewrites the manifest to
        # the new paths. On a pinned reference library that would both move
        # 24 GB and invalidate every recorded id, which is the opposite of what
        # pinning it was for.
        self._refuse_if_pinned("LocalizeContents")
        to_move = {}
        for path in self.manifest:
            if not path.is_absolute(): continue
            k = path.name
            to_move[k] = to_move.get(k, [])+[path]
        mover = Logistics()
        moved: list[tuple[Path, Path]] = []
        for dest, srcs in to_move.items():
            dest = Path(dest)
            plural = len(srcs)>1
            for i, src in enumerate(srcs):
                if plural:
                    dest_path = self.location/f"{dest.stem}_{i+1}{dest.suffix}"
                else:
                    dest_path = self.location/f"{dest.stem}{dest.suffix}"
                mover.QueueTransfer(src=Source.FromLocal(src), dest=Source.FromLocal(dest_path))
                moved.append((Path(src), dest_path.relative_to(self.location)))
        mover.ExecuteTransfers()
        for src, dest in moved:
            self.manifest[dest] = self.manifest[src]
            del self.manifest[src]
            if src in self.parents:
                self.parents[dest] = self.parents[src]
                del self.parents[src]
        return moved
