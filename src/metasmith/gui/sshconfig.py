# Metasmith holds no SSH configuration of its own -- every invocation is a bare
# `ssh <alias>` with no `-F` -- so the user's `~/.ssh/config` is the entire
# mechanism by which a remote host is reachable, and a project-local config would
# never be read.
#
# The managed block goes at the *top* of that file, and the placement is
# load-bearing: OpenSSH takes the first value it finds for each keyword, so a
# `Host *` block earlier in the file would set User or IdentityFile for a new
# alias below it -- an entry that parses cleanly, displays correctly, and
# connects as the wrong user.
from __future__ import annotations

import fnmatch
import os
import re
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

BEGIN_MARKER = "# >>> metasmith managed hosts >>>"
END_MARKER = "# <<< metasmith managed hosts <<<"

BLOCK_PREAMBLE = [
    "# Managed by metasmith. Everything between the markers is rewritten by the",
    "# GUI; keep the markers if you edit it by hand.",
    "#",
    "# This block is first in the file on purpose: ssh uses the first value it",
    "# finds for each keyword, so a wildcard block above it would silently win.",
]

FORM_KEYWORDS = [
    ("HostName", "hostname"),
    ("User", "user"),
    ("Port", "port"),
    ("ProxyJump", "proxy_jump"),
    ("IdentityFile", "identity_file"),
]

KEY_DIR_NAME = "metasmith"
KEY_TYPE = "ed25519"

_WILDCARD = re.compile(r"[*?!]")


class SshConfigError(Exception):
    pass
def default_config_path() -> Path:
    return Path(os.path.expanduser("~/.ssh/config"))


@dataclass
class HostEntry:
    pattern: str
    keywords: dict[str, str] = field(default_factory=dict)
    source: str = ""
    line: int = 0
    managed: bool = False

    @property
    def is_pattern(self) -> bool:
        return bool(_WILDCARD.search(self.pattern))

    def to_dict(self) -> dict:
        return {
            "alias": self.pattern,
            "hostname": self.keywords.get("hostname"),
            "user": self.keywords.get("user"),
            "port": self.keywords.get("port"),
            "proxy_jump": self.keywords.get("proxyjump"),
            "identity_file": self.keywords.get("identityfile"),
            "managed": self.managed,
            "source": self.source,
            "line": self.line,
            "keywords": dict(self.keywords),
        }


class SshConfig:
    def __init__(self, path: Path | str | None = None):
        self.path = Path(path) if path is not None else default_config_path()


    def read(self) -> str:
        if not self.path.is_file():
            return ""
        return self.path.read_text()

    def _write(self, text: str):
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        existed = self.path.exists()
        tmp = self.path.with_name(self.path.name + ".metasmith.tmp")
        tmp.write_text(text)
        os.chmod(tmp, 0o600)
        os.replace(tmp, self.path)
        if not existed:
            os.chmod(self.path, 0o600)


    def _resolve_include(self, token: str) -> list[Path]:
        token = os.path.expanduser(token)
        p = Path(token)
        if not p.is_absolute():
            p = self.path.parent / p
        matches = sorted(p.parent.glob(p.name)) if _WILDCARD.search(p.name) else ([p] if p.exists() else [])
        return [m for m in matches if m.is_file()]

    def _parse_file(self, path: Path, seen: set[Path], out: list[HostEntry], managed_ranges: dict[Path, list[tuple[int, int]]]):
        real = path.resolve()
        if real in seen or not path.is_file():
            return
        seen.add(real)

        lines = path.read_text().splitlines()
        ranges: list[tuple[int, int]] = []
        start = None
        for i, raw in enumerate(lines):
            if raw.strip() == BEGIN_MARKER:
                start = i
            elif raw.strip() == END_MARKER and start is not None:
                ranges.append((start, i))
                start = None
        managed_ranges[path] = ranges

        def _in_managed(i: int) -> bool:
            return any(a <= i <= b for a, b in ranges)

        current: list[HostEntry] = []
        in_match = False
        for i, raw in enumerate(lines):
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            parts = re.split(r"[\s=]+", line, maxsplit=1)
            key = parts[0].lower()
            value = parts[1].strip() if len(parts) > 1 else ""

            if key == "host":
                in_match = False
                current = []
                for pattern in value.split():
                    entry = HostEntry(
                        pattern=pattern,
                        source=str(path),
                        line=i + 1,
                        managed=_in_managed(i),
                    )
                    current.append(entry)
                    out.append(entry)
            elif key == "match":
                in_match = True
                current = []
            elif key == "include":
                for token in value.split():
                    for inc in self._resolve_include(token):
                        self._parse_file(inc, seen, out, managed_ranges)
            elif current and not in_match:
                for entry in current:
                    entry.keywords.setdefault(key, value)

    def entries(self) -> list[HostEntry]:
        out: list[HostEntry] = []
        self._parse_file(self.path, set(), out, {})
        return out

    @staticmethod
    def _collapse(entries: list[HostEntry]) -> list[HostEntry]:
        merged: dict[str, HostEntry] = {}
        for e in entries:
            first = merged.get(e.pattern)
            if first is None:
                merged[e.pattern] = HostEntry(
                    pattern=e.pattern,
                    keywords=dict(e.keywords),
                    source=e.source,
                    line=e.line,
                    managed=e.managed,
                )
                continue
            for k, v in e.keywords.items():
                first.keywords.setdefault(k, v)
        return list(merged.values())

    def resolved(self) -> list[HostEntry]:
        return self._collapse(self.entries())

    def hosts(self) -> list[dict]:
        return [e.to_dict() for e in self.resolved() if not e.is_pattern]

    def find(self, alias: str) -> HostEntry | None:
        for e in self.resolved():
            if e.pattern == alias:
                return e
        return None

    def shadowing_patterns(self, alias: str) -> list[dict]:
        return [
            e.to_dict() for e in self.resolved()
            if e.is_pattern and not e.managed and fnmatch.fnmatch(alias, e.pattern)
        ]


    def split(self) -> tuple[str, str, str]:
        text = self.read()
        if not text:
            return "", "", ""
        lines = text.splitlines()
        begin = end = None
        for i, raw in enumerate(lines):
            if raw.strip() == BEGIN_MARKER and begin is None:
                begin = i
            elif raw.strip() == END_MARKER and begin is not None:
                end = i
                break
        if begin is None or end is None:
            return text, "", ""
        before = "\n".join(lines[:begin])
        body = "\n".join(lines[begin + 1:end])
        after = "\n".join(lines[end + 1:])
        return before, body, after

    def managed_entries(self) -> list[dict]:
        return [e.to_dict() for e in self.entries() if e.managed and not e.is_pattern]


    @property
    def key_dir(self) -> Path:
        return self.path.parent / KEY_DIR_NAME

    def _tildify(self, p: Path) -> str:
        try:
            return "~/" + str(p.relative_to(Path.home()))
        except ValueError:
            return str(p)

    def resolve_identity(self, value: str | None) -> Path | None:
        if not value:
            return None
        return Path(os.path.expanduser(value.strip().strip('"')))

    def read_identity(self, value: str | None) -> dict | None:
        path = self.resolve_identity(value)
        if path is None:
            return None
        pub = path.with_name(path.name + ".pub")
        out = {
            "value": value,
            "path": str(path),
            "exists": path.is_file(),
            "generated": path.parent == self.key_dir,
            "public_key": None,
            "public_key_path": str(pub) if pub.is_file() else None,
        }
        if pub.is_file():
            try:
                out["public_key"] = pub.read_text().strip()
            except OSError:
                pass
        return out

    def generate_identity(self, alias: str, comment: str | None = None) -> dict:
        alias = alias.strip()
        assert alias, "an alias is required to name the key"
        assert not _WILDCARD.search(alias) and "/" not in alias, (
            f"[{alias}] is not usable as a key name"
        )

        dest = self.key_dir / alias
        if dest.is_file():
            out = self.read_identity(self._tildify(dest))
            return {**out, "created": False}

        if shutil.which("ssh-keygen") is None:
            raise SshConfigError(
                "ssh-keygen is not on PATH, so metasmith cannot generate a key. "
                "Make one yourself and point the identity field at it."
            )

        self.key_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        proc = subprocess.run(
            [
                "ssh-keygen", "-q",
                "-t", KEY_TYPE,
                "-N", "",
                "-C", comment or f"metasmith:{alias}",
                "-f", str(dest),
            ],
            capture_output=True, text=True,
        )
        if proc.returncode != 0 or not dest.is_file():
            detail = (proc.stderr or proc.stdout or "").strip()
            raise SshConfigError(f"ssh-keygen failed: {detail or 'no output'}")
        os.chmod(dest, 0o600)

        out = self.read_identity(self._tildify(dest))
        return {**out, "created": True}

    def delete_identity(self, alias: str) -> dict:
        alias = alias.strip()
        assert alias, "an alias is required"
        dest = self.key_dir / alias
        if not dest.is_file():
            raise SshConfigError(f"no generated key for [{alias}]")
        removed = []
        for p in (dest.with_name(dest.name + ".pub"), dest):
            if p.is_file():
                p.unlink()
                removed.append(str(p))
        return {"alias": alias, "action": "deleted", "removed": removed}

    @staticmethod
    def _strip_preamble(body: str) -> str:
        preamble = set(BLOCK_PREAMBLE)
        lines = body.splitlines()
        i = 0
        while i < len(lines) and (not lines[i].strip() or lines[i] in preamble):
            i += 1
        return "\n".join(lines[i:])

    def native(self) -> str:
        before, _managed, after = self.split()
        return "\n\n".join(x.strip("\n") for x in (before, after) if x.strip())

    def _compose(self, managed: str, native: str) -> str:
        block = "\n".join([
            BEGIN_MARKER, *BLOCK_PREAMBLE, self._strip_preamble(managed).strip("\n"), END_MARKER,
        ])
        text = block + "\n"
        if native.strip():
            text += "\n" + native.strip("\n") + "\n"
        return text

    def write_managed_block(self, body: str):
        self._write(self._compose(body, self.native()))

    def write_all(self, managed: str, native: str):
        for marker in (BEGIN_MARKER, END_MARKER):
            if marker in native:
                raise SshConfigError(
                    "the metasmith markers belong to the managed block; remove "
                    "them from the rest of the file before saving."
                )
        self._write(self._compose(managed, native))


    def _render(self, hosts: list[dict]) -> str:
        chunks = []
        for h in hosts:
            lines = [f"Host {h['alias']}"]
            for keyword, field_name in FORM_KEYWORDS:
                v = h.get(field_name)
                if v:
                    lines.append(f"    {keyword} {v}")
            for extra_k, extra_v in (h.get("extra") or {}).items():
                lines.append(f"    {extra_k} {extra_v}")
            chunks.append("\n".join(lines))
        return "\n\n".join(chunks)

    def add_host(
        self,
        alias: str,
        hostname: str,
        user: str | None = None,
        port: str | int | None = None,
        proxy_jump: str | None = None,
        identity_file: str | None = None,
    ) -> dict:
        alias = alias.strip()
        assert alias, "alias is required"
        assert not _WILDCARD.search(alias), (
            f"[{alias}] is a pattern, not a host; metasmith only manages concrete hosts"
        )
        assert hostname and hostname.strip(), "hostname is required"

        existing = self.find(alias)
        if existing is not None:
            where = f"{existing.source}:{existing.line}"
            raise SshConfigError(
                f"[{alias}] is already defined in [{where}]. "
                f"metasmith will not edit a host it does not own -- "
                f"pick another alias, or remove that entry yourself."
            )

        current = self.managed_entries()
        current.append({
            "alias": alias,
            "hostname": hostname.strip(),
            "user": (user or "").strip() or None,
            "port": str(port).strip() if port not in (None, "") else None,
            "proxy_jump": (proxy_jump or "").strip() or None,
            "identity_file": (identity_file or "").strip() or None,
        })
        self.write_managed_block(self._render(current))
        return self.find(alias).to_dict()

    def update_host(self, alias: str, /, **fields) -> dict:
        entry = self.find(alias)
        if entry is None:
            raise SshConfigError(f"no host named [{alias}]")
        if not entry.managed:
            raise SshConfigError(
                f"[{alias}] is defined in [{entry.source}:{entry.line}], outside the "
                f"metasmith block. Edit it there, or in the config editor."
            )
        new_alias = str(fields.get("alias") or alias).strip()
        if new_alias != alias:
            assert not _WILDCARD.search(new_alias), (
                f"[{new_alias}] is a pattern, not a host; metasmith only manages "
                f"concrete hosts"
            )
            clash = self.find(new_alias)
            if clash is not None:
                raise SshConfigError(
                    f"[{new_alias}] is already defined in "
                    f"[{clash.source}:{clash.line}]; pick another alias"
                )
        hosts = self.managed_entries()
        for h in hosts:
            if h["alias"] != alias:
                continue
            for k in ("hostname", "user", "port", "proxy_jump", "identity_file"):
                if k in fields:
                    v = fields[k]
                    h[k] = str(v).strip() or None if v not in (None, "") else None
            h["alias"] = new_alias
        self.write_managed_block(self._render(hosts))
        return self.find(new_alias).to_dict()

    def remove_host(self, alias: str) -> dict:
        entry = self.find(alias)
        if entry is None:
            raise SshConfigError(f"no host named [{alias}]")
        if not entry.managed:
            raise SshConfigError(
                f"[{alias}] is defined in [{entry.source}:{entry.line}], outside the "
                f"metasmith block; it is not metasmith's to remove."
            )
        hosts = [h for h in self.managed_entries() if h["alias"] != alias]
        self.write_managed_block(self._render(hosts))
        return {"alias": alias, "action": "removed"}
