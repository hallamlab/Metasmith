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

@dataclass
class SshSource:
    host: str
    path: Path
    _prefix = "ssh://"
    
    def __str__(self) -> str:
        return self._prefix+f"{self.host}:{self.path}"
    
    def __repr__(self) -> str:
        return f"{self}"

    @classmethod
    def Parse(cls, address: str):
        if address.startswith(cls._prefix):
            address = address[len(cls._prefix):]
            toks = address.split(":")
            host = toks[0]
            path = ":".join(toks[1:])
            return cls(host=host, path=Path(path))
        raise ValueError(f"not an ssh address [{address}]")
    
    def CompileAddress(self):
        if self.host == "":
            return str(self.path)
        else:
            return f"{self.host}:{self.path}"

    def AsSource(self):
        return Source(address=str(self), type=SourceType.SSH)

@dataclass
class HttpSource:
    url: str
    def __str__(self) -> str:
        return f"{self.url}"
    
    def __repr__(self) -> str:
        return f"{self}"
    
    @classmethod
    def Parse(cls, address: str):
        if any(address.startswith(pre) for pre in ["http://", "https://", "ftp://"]): # yes yes, ftp is not http
            return cls(url=address)
        raise ValueError(f"not an http(s) address [{address}]")
    
    def AsSource(self):
        return Source(address=str(self), type=SourceType.HTTP)

# transfer priority
class SourceType(Enum):
    GLOBUS =    "globus"
    SSH =       "ssh"
    HTTP =      "http"
    SYMLINK =   "symlink"
    DIRECT =    "direct"

    def __str__(self) -> str:
        return f"SourceType.{self.name}"
    
    def __repr__(self) -> str:
        return f"{self}"

@dataclass
class Source:
    address: str
    type: SourceType = SourceType.DIRECT

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

    def Clone(self):
        return Source(address=self.address, type=self.type)

    @classmethod
    def FromLocal(cls, path: Path|str):
        if isinstance(path, str): path = Path(path)
        assert path.is_absolute(), f"Path must be absolute [{path}]"
        t = SourceType.SYMLINK if path.is_symlink() else SourceType.DIRECT
        return Source(address=str(path), type=t)

    def GetName(self, extension: bool = True):
        p = self.GetPath()
        return p.stem if not extension else p.name
    
    def GetPath(self):
        return Path(self.address.split(":")[-1])

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

@dataclass
class LogiscsResult:
    completed: list[tuple[Source, Source]]
    errors: list[str]

class Logistics:
    def __init__(self) -> None:
        self._queue: list[tuple[Source, Source]] = []

    def _check_pures(self, src: Source, dest: Source):
        # illegal destination types
        assert dest.type not in {SourceType.HTTP}, f"cannot transfer to [{dest.type}]"
        
        # transfers including cloud must use the same platform
        CLOUD_TYPES = {SourceType.GLOBUS, SourceType.SSH, SourceType.HTTP}
        specified_cloud_types = {x for x in [src.type, dest.type] if x in CLOUD_TYPES}
        assert len(specified_cloud_types) <= 1, f"cannot transfer between [{src.type} -> {dest.type}]"
        
        if len(specified_cloud_types) == 0:
            dominant = SourceType.DIRECT # including symlinks
        else:
            dominant = next(iter(specified_cloud_types))

            # cloud <-> local must be direct
            cloud, other = (src, dest) if src.type in CLOUD_TYPES else (dest, src)
            assert other in CLOUD_TYPES or other.type == SourceType.DIRECT, f"transfer involves [{cloud.type}] so [{other.type} must be {SourceType.DIRECT}"

        return dominant

    def QueueTransfer(self, src: Source, dest: Source):
        self._check_pures(src, dest)
        self._queue.append((src, dest))

    def RemoveTransfer(self, src: Source, dest: Source):
        self._queue.remove((src, dest))

    def ExecuteTransfers(self, label: str = None) -> LogiscsResult:
        to_dispose: list[LiveShell] = []
        result = LogiscsResult(completed=[], errors=[])

        with TemporaryDirectory(prefix="msm.") as tmpdir:
            def _execute_local(todo: list[tuple[Source, Source]]):
                shell = LiveShell()
                shell._start()
                shell.RegisterOnErr(lambda x: result.errors.append(f"local: {x}"))
                to_dispose.append(shell)
                for src, dest in todo:
                    dest_path = Path(dest.address)
                    if dest_path.exists() and (dest_path.is_symlink() != (dest.type == SourceType.SYMLINK)):
                        dest_path.unlink()
                    if dest.type == SourceType.SYMLINK:
                        shell.ExecAsync(f"ln -s {src.address} {dest.address}")
                    elif dest.type == SourceType.DIRECT:
                        shell.ExecAsync(f"rsync -au {src.address}/ {dest.address} 2>/dev/null || rsync -au {src.address} {dest.address}")

                def _join():
                    shell.AwaitDone(timeout=None)
                    completed = []
                    for src, dest in todo:
                        dest_path = Path(dest.address)
                        if not dest_path.exists(): continue
                        completed.append((src, dest))
                return _join

            def _execute_globus(todo: list[tuple[Source, Source]]):
                def _to_globus(s: Source):
                    if s.type == SourceType.GLOBUS:
                        return GlobusSource.Parse(s.address)
                    else:
                        path = Path(s.address)
                        assert path.is_absolute()
                        return GlobusSource(endpoint=_get_globus_local_id(), path=str(path))
                batched_globus: dict[tuple[str, str], list[tuple[GlobusSource, GlobusSource, Source, Source]]] = {}
                for src, dest in todo:
                    try:
                        src_g = _to_globus(src)
                        dest_g = _to_globus(dest)
                    except (ValueError, AssertionError) as e:
                        result.errors.append(f"failed to convert to GlobusSource: [{e}] [{src}] [{dest}]")
                        continue
                    key = src_g.endpoint, dest_g.endpoint
                    batch = batched_globus.get(key, [])
                    batch.append((src_g, dest_g, src, dest))
                    batched_globus[key] = batch

                tasks = []
                with LiveShell() as shell:
                    for (src_ep, dest_ep), batch in batched_globus.items():
                        batch_path = Path(tmpdir)/"batch"
                        with open(batch_path, "w") as f:
                            for src_g, dest_g, _, _ in batch:
                                f.write(f"{src_g.path} {dest_g.path}\n")
                        cmd = f"globus transfer {src_ep} {dest_ep} --batch {batch_path} --sync-level checksum" + (f" --label {label}" if label else "")
                        res = shell.Exec(cmd, history=True)
                        _kw = "Task ID: "
                        _task_ids = [x.replace(_kw, "") for x in res.out if x.startswith(_kw)]
                        assert len(_task_ids) == 1, f"globus transfer failed to submit"
                        _task_id = _task_ids[0]
                        tasks.append((_task_id, [(src, dest) for _, _, src, dest in batch]))

                def _join():
                    completed = []
                    try:
                        _last_len = -1
                        while len(tasks) > 0:
                            if len(tasks) != _last_len:
                                _last_len = len(tasks)
                            _task, batch = tasks[-1]
                            res = shell.Exec(f"globus task show {_task} -F json", history=True)
                            try:
                                d = json.loads("\n".join(res.out))
                            except json.JSONDecodeError as e:
                                result.errors.append(f"globus poll json error: [{'\n'.join(res.out)}]")
                                tasks.pop(-1)
                                continue
                            status = d.get("status")
                            if status not in {"ACTIVE"}:
                                completed += batch
                                tasks.pop(-1) # completed
                                continue
                            time.sleep(1)
                    finally:
                        for k, _ in tasks:
                            shell.ExecAsync(f"globus task cancel {k}")
                        shell.AwaitDone()
                    return completed
                return _join
            
            def _execute_ssh(todo: list[tuple[Source, Source]]):
                def _to_ssh(s: Source):
                    if s.type == SourceType.SSH:
                        return SshSource.Parse(s.address)
                    else:
                        path = Path(s.address)
                        assert path.is_absolute()
                        return SshSource(host="", path=str(path))

                batched_ssh: dict[tuple[str, str], list[tuple[SshSource, SshSource, Source, Source]]] = {}
                for src, dest in todo:
                    try:
                        src_s = _to_ssh(src)
                        dest_s = _to_ssh(dest)
                    except (ValueError, AssertionError) as e:
                        result.errors.append(f"failed to convert to SshSource: [{e}] [{src}] [{dest}]")
                        continue
                    key = src_s.host, dest_s.host
                    batch = batched_ssh.get(key, [])
                    batch.append((src_s, dest_s, src, dest))
                    batched_ssh[key] = batch

                shell = LiveShell()
                shell._start()
                shell.RegisterOnErr(lambda x: result.errors.append(f"ssh: {x}"))
                to_dispose.append(shell)
                for (src_host, dest_host), batch in batched_ssh.items():
                    for src_s, dest_s, _, _ in batch:
                        src_addr, dest_addr = src_s.CompileAddress(), dest_s.CompileAddress()
                        shell.ExecAsync(f"rsync -au {src_addr}/ {dest_addr} 2>/dev/null || rsync -au {src_addr} {dest_addr}")
                def _join():
                    shell.AwaitDone(timeout=None)
                    completed = []
                    for (src_host, dest_host), batch in batched_ssh.items():
                        def _check(path: str):
                            if dest_host != "":
                                FLAG = "ok"
                                res = shell.Exec(f'[ -e {path} ] && echo "{FLAG}"', history=True)
                                return FLAG in res.out
                            else:
                                return Path(path).exists()

                        if dest_host != "":
                            shell.Exec(f'ssh {dest_host}') # todo: what if ssh fails?
                        for _, dest_s, src, dest in batch:
                            if _check(dest_s.path):
                                completed.append((src, dest))
                        if dest_host != "":
                            shell.Exec("exit")
                    return completed
                return _join

            def _execute_http(todo: list[tuple[Source, Source]]):
                shell = LiveShell()
                shell._start()
                shell.RegisterOnErr(lambda x: result.errors.append(f"http: {x}"))
                to_dispose.append(shell)
                for src, dest in todo:
                    dest_path = Path(dest.address)
                    shell.ExecAsync(f"mkdir -p {dest_path.parent} && curl -C - --silent -o {dest_path} {src.address}")
                def _join():
                    shell.AwaitDone(timeout=None)
                    completed = []
                    for src, dest in todo:
                        if Path(dest.address).exists():
                            completed.append((src, dest))
                    return completed
                return _join

            by_type = {}
            for src, dest in self._queue:
                type_category = self._check_pures(src, dest)
                by_type[type_category] = by_type.get(type_category, [])+[(src, dest)]
            _order = list(SourceType)
            _t2i = {t: i for i, t in enumerate(_order)}
            by_type = {k: v for k, v in sorted(by_type.items(), key=lambda x: _t2i[x[0]])}
            try:
                _joiners = []
                for type_category, todo in by_type.items():
                    exe = { # switch
                        SourceType.DIRECT: _execute_local,
                        SourceType.GLOBUS: _execute_globus,
                        SourceType.SSH: _execute_ssh,
                        SourceType.HTTP: _execute_http,
                    }[type_category]
                    _joiners.append(exe(todo))
                for fn in _joiners:
                    result.completed += fn()
            finally:
                for d in to_dispose:
                    d._stop()
            return result
