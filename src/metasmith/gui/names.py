from __future__ import annotations

import re
from collections.abc import Iterable

from ..hashing import KeyGenerator

NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
RUN_SUFFIX_LENGTH = 5


def is_valid_name(name: str) -> bool:
    return bool(NAME_PATTERN.match(name)) and name not in {".", ".."}


def assert_valid_name(name: str, what: str = "name"):
    assert is_valid_name(name), (
        f"invalid {what} [{name}]: use lowercase letters, digits, dot, dash or "
        f"underscore, starting with a letter or digit, up to 64 characters"
    )


def slugify(text: str) -> str:
    s = text.strip().lower()
    s = re.sub(r"[^a-z0-9._-]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-._")
    return s[:64]


def generate_workflow_name(taken: Iterable[str] = ()) -> str:
    import coolname

    taken = set(taken)
    for _ in range(64):
        name = coolname.generate_slug(2)
        if name not in taken:
            return name
    for _ in range(64):
        name = coolname.generate_slug(3)
        if name not in taken:
            return name
    raise AssertionError("could not generate an unused workflow name")


AGENT_LOCAL_HOST = "local"


def compose_agent_name(prefix: str, host: str) -> str:
    return slugify(f"{prefix}-{host}")


def agent_sort_name(prefix: str, host: str) -> str:
    return f"{slugify(host)}{slugify(prefix)}"


def generate_agent_name(host: str, taken: Iterable[str] = ()) -> tuple[str, str, str]:
    import coolname

    taken = set(taken)
    for _ in range(64):
        prefix = coolname.generate(2)[0]
        name = compose_agent_name(prefix, host)
        if name not in taken and is_valid_name(name):
            return prefix, name, agent_sort_name(prefix, host)
    for _ in range(64):
        prefix = coolname.generate_slug(2)
        name = compose_agent_name(prefix, host)
        if name not in taken and is_valid_name(name):
            return prefix, name, agent_sort_name(prefix, host)
    raise AssertionError(f"could not generate an unused agent name for host [{host}]")


def generate_run_name(workflow_name: str, taken: Iterable[str] = ()) -> str:
    taken = set(taken)
    suffixes = {n.rsplit("-", 1)[-1] for n in taken}
    suffix = KeyGenerator().GenerateUID(l=RUN_SUFFIX_LENGTH, blacklist=suffixes)
    return f"{workflow_name}-{suffix}"
