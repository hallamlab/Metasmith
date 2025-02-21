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
from ..hashing import KeyGenerator
from ..logging import Log

_globus_domain2uuid: dict[str, str] = {}
_globus_local_id: str = None
def _get_globus_local_id():
    global _globus_local_id
    if _globus_local_id is None:
        with LiveShell() as shell:
            res = shell.Exec("globus endpoint local-id", history=True)
            out, err = res.out, res.err
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
        
        def _try_get(d, ks):
            for k in ks:
                if k in d:
                    return d[k]
            return None

        url = urlparse(address)
        if url.netloc == "app.globus.org":
            qs = parse_qs(url.query)
            _ep = _try_get(qs, ["origin_id", "destination_id"])[0]
            _path = _try_get(qs, ["origin_path", "destination_path"])[0]
            return cls(endpoint=_ep, path=Path(_path))
        elif re.match(r"^g-[\w\.]+\.data\.globus\.org$", url.netloc):
            if url.netloc in _globus_domain2uuid:
                return cls(_globus_domain2uuid[url.netloc], url.path)
            with LiveShell() as shell:
                res = shell.Exec(f"globus endpoint search {url.netloc} -F json", history=True)
                assert len(err) == 0, "\n".join(res.err)
                data = json.loads("\n".join(res.out))["DATA"]
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
    
    @classmethod
    def FromSource(cls, source: Source):
        if source.type == SourceType.GLOBUS:
            return cls.Parse(source.address)
        return cls.FromLocalPath(source.address)
    
    def AsSource(self):
        return Source(address=str(self), type=SourceType.GLOBUS)

class SourceType(Enum):
    DIRECT = "direct"
    SYMLINK = "symlink"
    GLOBUS = "globus"

    def __str__(self) -> str:
        return f"SourceType.{self.name}"
    
    def __repr__(self) -> str:
        return f"{self}"

@dataclass
class Source:
    address: str
    type: SourceType = SourceType.DIRECT
    _hash: int = None

    def __post_init__(self):
        if not isinstance(self.address, str):
            self.address = str(self.address)
        h, k = KeyGenerator.FromStr(f"{self.address}{self.type}")
        self._hash = h

    def __hash__(self) -> int:
        return self._hash

    def __truediv__(self, other: str|Path):
        if isinstance(other, Path):
            assert not other.is_absolute()
            other = str(other)
        address = self.address
        if address.endswith("/"):
            address = address[:-1]
        if other.startswith("/"):
            other = other[1:]
        return Source(address=f"{address}/{other}", type=self.type)

    def GetName(self, extension: bool = True):
        p = Path(self.address.split(":")[-1])
        return p.stem if not extension else p.name

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

    def ExecuteTransfers(self, label: str = None):
        locals: list[tuple[Source, Source]] = []
        globus: list[tuple[Source, Source]] = []
        Log.Info(f"starting [{len(self._queue)}] transfers")
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
            shell.RegisterOnErr(Log.Error)
            shell.RegisterOnOut(Log.Info)
            task_ids = []
            try:
                # start globus transfers
                for (src_ep, dest_ep), batch in batched_globus.items():
                    Log.Info(f"executing globus batch of [{len(batch)}] for [{src_ep}] -> [{dest_ep}]")
                    for src, dest in batch:
                        Log.Info(f"[{src.path}] -> [{dest.path}]")
                        shell.ExecAsync(f'echo "\"{src.path}\" \"{dest.path}\"" >> {tmpdir}/batch')
                    shell.AwaitDone()
                    cmd = f"globus transfer {src_ep} {dest_ep} --batch {tmpdir}/batch --sync-level checksum" + (f" --label {label}" if label else "")
                    res = shell.Exec(cmd, history=True)
                    errs.extend(f"globus batch std_err: [{e}]" for e in res.err)
                    _kw = "Task ID: "
                    _task_ids = [x.replace(_kw, "") for x in res.out if x.startswith(_kw)]
                    assert len(_task_ids) == 1, f"globus transfer failed to submit"
                    task_ids.append(_task_ids[0])
            
                # do local transfers in meantime
                def _local_transfer_err(e):
                    errs.append(f"local transfer error: [{e}]")
                shell.RegisterOnErr(_local_transfer_err)
                _hashes = {}
                def _checksum(p):
                    if p not in _hashes:
                        res = shell.Exec(f"shasum -a 256 {p}", history=True)
                        if len(res.err)>0:
                            errs.extend(f"checksum error: [{e}]" for e in res.err)
                            return
                        _hashes[p] = res.out[0].split(" ")[0]
                    return _hashes[p]
                if len(locals)>0: Log.Info(f"executing [{len(locals)}] local transfers")
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
                _last_len = -1
                shell.RemoveOnOut(Log.Info) # will print out json otherwise
                while len(task_ids) > 0:
                    if len(task_ids) != _last_len:
                        _last_len = len(task_ids)
                        Log.Info(f"awaiting [{_last_len}] globus transfers")
                    _task = task_ids[-1]
                    res = shell.Exec(f"globus task show {_task} -F json", history=True)
                    errs.extend(f"globus poll std_err: [{e}]" for e in res.err)
                    try:
                        d = json.loads("\n".join(res.out))
                    except json.JSONDecodeError as e:
                        errs.extend(f"globus poll json error: [{x}]" for x in res.out)
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
            Log.Info(f"attempted [{len(self._queue)}] transfers")
        return errs
