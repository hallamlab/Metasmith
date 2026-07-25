"""Reading and owning a slice of the user's OpenSSH config.

Metasmith itself holds no SSH configuration: a remote source is a host alias and
a path, and every invocation is a bare `ssh <alias>` with no user, port, key, or
jump host and no `-F`. The user's own config is therefore the entire mechanism by
which a remote host is reachable, and a project-local config would simply never
be read -- which is why this writes to `~/.ssh/config` rather than somewhere
tidier.

Ownership is delimited by comment markers, in the style of a conda shell
initialisation block. Everything inside the markers is metasmith's to rewrite;
everything outside it is the user's, and is shown read-only. Creating a host that
is already declared elsewhere is refused rather than merged, so the two never
fight over the same alias.

The block goes at the *top* of the file, and that placement is load-bearing.
OpenSSH takes the first value it finds for each keyword, so a `Host *` block
earlier in the file would set User or IdentityFile for a brand-new alias below
it -- producing an entry that parses cleanly, displays correctly, and connects as
the wrong user.
"""
from __future__ import annotations

import fnmatch
import os
import re
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

# the four fields the form carries; anything else belongs in the text editor.
# (ssh keyword, field name)
FORM_KEYWORDS = [
    ("HostName", "hostname"),
    ("User", "user"),
    ("Port", "port"),
    ("ProxyJump", "proxy_jump"),
]

_WILDCARD = re.compile(r"[*?!]")


class SshConfigError(Exception):
    """A refusal the GUI should show to the user rather than a bug."""


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
            "managed": self.managed,
            "source": self.source,
            "line": self.line,
            "keywords": dict(self.keywords),
        }


class SshConfig:
    def __init__(self, path: Path | str | None = None):
        self.path = Path(path) if path is not None else default_config_path()

    # -- raw file ----------------------------------------------------------

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

    # -- parsing -----------------------------------------------------------

    def _resolve_include(self, token: str) -> list[Path]:
        token = os.path.expanduser(token)
        p = Path(token)
        if not p.is_absolute():
            # OpenSSH resolves relative Include paths against ~/.ssh
            p = self.path.parent / p
        matches = sorted(p.parent.glob(p.name)) if _WILDCARD.search(p.name) else ([p] if p.exists() else [])
        return [m for m in matches if m.is_file()]

    def _parse_file(self, path: Path, seen: set[Path], out: list[HostEntry], managed_ranges: dict[Path, list[tuple[int, int]]]):
        real = path.resolve()
        if real in seen or not path.is_file():
            return
        seen.add(real)

        lines = path.read_text().splitlines()
        # find managed marker ranges so entries can be attributed to metasmith
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
            # `key value` or `key=value`
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
                # a Match block is conditional; its keywords are not this host's
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

    def hosts(self) -> list[dict]:
        """Concrete destinations only.

        Wildcard patterns are defaults applied to other hosts, not things you can
        connect to, so they are not offered as agent homes.
        """
        return [e.to_dict() for e in self.entries() if not e.is_pattern]

    def find(self, alias: str) -> HostEntry | None:
        for e in self.entries():
            if e.pattern == alias:
                return e
        return None

    def shadowing_patterns(self, alias: str) -> list[dict]:
        """Wildcard blocks that would apply to `alias`.

        Informational: the managed block is written first so these cannot
        override it, but the user should still be able to see them.
        """
        return [
            e.to_dict() for e in self.entries()
            if e.is_pattern and not e.managed and fnmatch.fnmatch(alias, e.pattern)
        ]

    # -- the managed block -------------------------------------------------

    def split(self) -> tuple[str, str, str]:
        """(before, managed_body, after) of the top-level config file."""
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

    def write_managed_block(self, body: str):
        """Replace the managed block, always placing it first in the file."""
        before, _current, after = self.split()
        rest = "\n".join(x for x in (before, after) if x.strip())
        block = "\n".join([BEGIN_MARKER, *BLOCK_PREAMBLE, body.strip("\n"), END_MARKER])
        text = block + "\n"
        if rest.strip():
            text += "\n" + rest.rstrip("\n") + "\n"
        self._write(text)

    # -- host CRUD ---------------------------------------------------------

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
        })
        self.write_managed_block(self._render(current))
        return self.find(alias).to_dict()

    def update_host(self, alias: str, **fields) -> dict:
        entry = self.find(alias)
        if entry is None:
            raise SshConfigError(f"no host named [{alias}]")
        if not entry.managed:
            raise SshConfigError(
                f"[{alias}] is defined in [{entry.source}:{entry.line}], outside the "
                f"metasmith block. Edit it there, or in the config editor."
            )
        hosts = self.managed_entries()
        for h in hosts:
            if h["alias"] != alias:
                continue
            for k in ("hostname", "user", "port", "proxy_jump"):
                if k in fields:
                    v = fields[k]
                    h[k] = str(v).strip() or None if v not in (None, "") else None
        self.write_managed_block(self._render(hosts))
        return self.find(alias).to_dict()

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
