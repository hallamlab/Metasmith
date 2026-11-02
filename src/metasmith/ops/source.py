from __future__ import annotations

import subprocess
import urllib.request
from pathlib import Path

from ..models.remote import Source, SourceType, Logistics


def parse(uri: str) -> dict:
    src = Source.Parse(uri)
    return {
        "address": src.address,
        "type": src.type.name,
        "name": src.GetName(),
        "path": str(src.GetPath()),
    }


def exists(uri: str, timeout_s: int = 10) -> dict:
    src = Source.Parse(uri)
    is_present = False
    detail: str = ""
    if src.type in (SourceType.DIRECT, SourceType.SYMLINK):
        is_present = Path(src.address).exists()
        detail = "filesystem"
    elif src.type == SourceType.SSH:
        from ..models.remote import SshSource
        ssh = SshSource.Parse(src.address)
        cmd = [
            "ssh", "-o", f"ConnectTimeout={timeout_s}", "-o", "BatchMode=yes",
            ssh.host, f"test -e {ssh.path} && echo OK",
        ]
        try:
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s + 5)
            is_present = "OK" in res.stdout
            detail = f"ssh rc={res.returncode}"
        except Exception as exc:
            detail = f"ssh error: {exc}"
    elif src.type == SourceType.HTTP:
        try:
            req = urllib.request.Request(src.address, method="HEAD")
            with urllib.request.urlopen(req, timeout=timeout_s) as r:
                is_present = 200 <= r.status < 400
                detail = f"http {r.status}"
        except Exception as exc:
            detail = f"http error: {exc}"
    else:
        detail = f"probe not implemented for {src.type.name}"
    return {"uri": uri, "exists": is_present, "type": src.type.name, "detail": detail}


def transfer(src_uri: str, dest_uri: str, wait: bool = True, label: str | None = None) -> dict:
    src = Source.Parse(src_uri)
    dest = Source.Parse(dest_uri)
    mover = Logistics()
    mover.QueueTransfer(src=src, dest=dest)
    res = mover.ExecuteTransfers(label, wait)
    return {
        "completed": [(s.address, d.address) for s, d in res.completed],
        "errors": list(res.errors),
    }
