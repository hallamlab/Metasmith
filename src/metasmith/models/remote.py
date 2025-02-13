from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
import time
from urllib.parse import urlparse, parse_qs
import re
import json
from tempfile import TemporaryDirectory

from ..coms.ipc import LiveShell

_globus_domain2uuid: dict[str, str] = {}
_globus_local_id: str = None
def _get_globus_local_id():
    global _globus_local_id
    if _globus_local_id is None:
        with LiveShell() as shell:
            out, err = shell.Exec("globus endpoint local-id", history=True)
            assert len(err) == 0, "\n".join(err)
            assert len(out) == 1, "\n".join(out)
            _globus_local_id = out[0].strip()
    return _globus_local_id

# if in container, globus cli in container while login credentials are mounted
# since the local endpoint is resolved, do not cache
@dataclass
class GlobusSource:
    endpoint: str
    path: Path
    _prefix = "globus://"
    
    def __str__(self) -> str:
        return self._prefix+f"{self.endpoint}:{self.path}"
    
    def __repr__(self) -> str:
        return f"{self}"
    
    @classmethod
    def Parse(cls, address: str):
        if address.startswith(cls._prefix):
            address = address[len(cls._prefix):]
            toks = address.split(":")
            endpoint = toks[0]
            path = ":".join(toks[1:])
            return cls(endpoint=endpoint, path=Path(path))
        err = ValueError(f"not a globus address [{address}]")
        if not re.match(r"^https?://", address):
            raise err
        
        url = urlparse(address)
        if url.netloc == "app.globus.org":
            qs = parse_qs(url.query)
            origin_id = qs["origin_id"][0]
            origin_path = qs["origin_path"][0]
            return cls(endpoint=origin_id, path=Path(origin_path))
        elif re.match(r"^g-[\w\.]+\.data\.globus\.org$", url.netloc):
            if url.netloc in _globus_domain2uuid:
                return cls(_globus_domain2uuid[url.netloc], url.path)
            with LiveShell() as shell:
                out, err = shell.Exec(f"globus endpoint search {url.netloc} -F json", history=True)
                assert len(err) == 0, "\n".join(err)
                data = json.loads("\n".join(out))["DATA"]
                assert len(data)>0, f"No endpoint found for [{url.netloc}]"
                uuid = data[0]["id"]
                _globus_domain2uuid[url.netloc] = uuid
                return cls(endpoint=uuid, path=Path(url.path))
        raise err
    
    @classmethod
    def FromLocalPath(cls, path: Path|str):
        path = Path(path)
        assert path.is_absolute(), f"Path must be absolute [{path}]"
        return cls(_get_globus_local_id(), str(path))

class SourceType(Enum):
    DIRECT = "direct"
    SYMLINK = "symlink"
    GLOBUS = "globus"

@dataclass
class Source:
    address: str
    type: SourceType

    def __post_init__(self):
        if not isinstance(self.address, str):
            self.address = str(self.address)

    @classmethod
    def Unpack(cls, d: dict):
        return cls(
            address=d["address"],
            type=SourceType[d["type"]],
        )

    def Pack(self):
        return {
            "address": self.address,
            "type": self.type.name,
        }

class Logistics:
    def __init__(self) -> None:
        self._queue: list[tuple[Source, Source]] = []

    def QueueTransfer(self, src: Source, dest: Source):
        self._queue.append((src, dest))

    def ExecuteTransfers(self):
        locals = []
        globus: list[tuple[GlobusSource, GlobusSource]] = []
        for src, dest in self._queue:
            if SourceType.GLOBUS in (src.type, dest.type):
                globus.append((src, dest))
            else:
                locals.append((src, dest))

        batched_globus: dict[tuple[str, str], list[tuple[GlobusSource, GlobusSource]]] = {}
        for src, dest in globus:
            key = src.endpoint, dest.endpoint
            batch = batched_globus.get(key, [])
            batch.append((src, dest))
            batched_globus[key] = batch

        with LiveShell() as shell, TemporaryDirectory(prefix="msm.") as tmpdir:
            for (src_ep, dest_ep), batch in batched_globus.items():
                for src, dest in batch:
                    shell.ExecAsync(f'echo "\"{src.path}\" \"{dest.path}\"" >> {tmpdir}/batch')
                shell.Exec(f"globus transfer --batch {tmpdir}/batch --source-endpoint {src_ep} --destination-endpoint {dest_ep}", history=True)

class Logistics:
    def __init__(self) -> None:
        self._queue: list[tuple[Source, Source]] = []

    def QueueTransfer(self, src: Source, dest: Source):
        self._queue.append((src, dest))

    def ExecuteTransfers(self, label: str = None):
        locals: list[tuple[Source, Source]] = []
        globus: list[tuple[Source, Source]] = []
        for src, dest in self._queue:
            if SourceType.GLOBUS in (src.type, dest.type):
                globus.append((src, dest))
            else:
                locals.append((src, dest))

        def _to_globus(s: Source):
            if s.type == SourceType.GLOBUS:
                return GlobusSource.Parse(s.address)
            else:
                path = Path(s.address)
                assert path.is_absolute()
                return GlobusSource(endpoint=_get_globus_local_id(), path=str(path))

        batched_globus: dict[tuple[str, str], list[tuple[GlobusSource, GlobusSource]]] = {}
        errs: list[str] = []
        for src, dest in globus:
            try:
                src = _to_globus(src)
                dest = _to_globus(dest)
            except (ValueError, AssertionError) as e:
                errs.append(f"skipped [{src}, {dest}] due to parse error [{e}]")
                continue
            key = src.endpoint, dest.endpoint
            batch = batched_globus.get(key, [])
            batch.append((src, dest))
            batched_globus[key] = batch

        with LiveShell() as shell, TemporaryDirectory(prefix="msm.") as tmpdir:
            task_ids = []
            try:
                # start globus transfers
                for (src_ep, dest_ep), batch in batched_globus.items():
                    for src, dest in batch:
                        shell.ExecAsync(f'echo "\"{src.path}\" \"{dest.path}\"" >> {tmpdir}/batch')
                    shell.AwaitDone()
                    out, err = shell.Exec(f"globus transfer {src_ep} {dest_ep} --batch {tmpdir}/batch --sync-level checksum" + f" --label {label}" if label else "", history=True)
                    errs.extend(f"globus batch std_err: [{e}]" for e in err)
                    _kw = "Task ID: "
                    task_ids.extend(x.replace(_kw, "") for x in out if x.startswith(_kw)) # should only be 1
            
                # do local transfers in meantime
                def _local_transfer_err(e):
                    errs.append(f"local transfer error: [{e}]")
                shell.RegisterOnErr(_local_transfer_err)
                _hashes = {}
                def _checksum(p):
                    if p not in _hashes:
                        out, err = shell.Exec(f"shasum -a 256 {p}", history=True)
                        if len(err)>0:
                            errs.extend(f"checksum error: [{e}]" for e in err)
                            return
                        _hashes[p] = out[0].split(" ")[0]
                    return _hashes[p]
                for src, dest in locals:
                    dest_path = Path(dest.address)
                    if dest_path.exists():
                        if dest_path.is_symlink() and (dest.type == SourceType.SYMLINK): continue
                        if _checksum(src.address) == _checksum(dest_path): continue
                        dest_path.unlink()
                    if dest.type == SourceType.SYMLINK:
                        shell.ExecAsync(f"ln -s {src.address} {dest.address}")
                    elif dest.type == SourceType.DIRECT:
                        shell.ExecAsync(f"cp {src.address} {dest.address}")
                shell.AwaitDone(timeout=None)
                shell.RegisterOnErr(_local_transfer_err)
                
                # wait for globus transfers to complete
                while len(task_ids) > 0:
                    _task = task_ids[-1]
                    out, err = shell.Exec(f"globus task show {_task} -F json", history=True)
                    errs.extend(f"globus poll std_err: [{e}]" for e in err)
                    try:
                        d = json.loads("\n".join(out))
                    except json.JSONDecodeError as e:
                        errs.extend(f"globus poll json error: [{x}]" for x in out)
                        task_ids.remove(_task)
                        continue
                    status = d.get("status")
                    if status not in {"ACTIVE"}:
                        task_ids.remove(_task)
                        continue
                    time.sleep(1)
            finally:
                for k in task_ids:
                    shell.ExecAsync(f"globus task cancel {k}")
                shell.AwaitDone()
        return errs
