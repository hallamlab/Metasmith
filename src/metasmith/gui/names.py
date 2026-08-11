"""Readable names for workflows, runs and agents.

Task keys are content-derived and stable, which is what makes them useful for
caching and useless for conversation. These are the names people actually say
out loud: a workflow is "blazing-ape", and each of its runs is "blazing-ape-0XwE9".
The key is still the identity; the name is the handle.

An agent is the exception, because there is a second thing worth knowing about
one: which machine it is. So an auto-named agent is "blazing-sockeye" -- one word
and the host -- and carries a *sort* name, "sockeyeblazing", so that a list of
them groups by machine without anyone having to name them carefully.
"""
from __future__ import annotations

import re
from collections.abc import Iterable

from ..hashing import KeyGenerator

# a name is what a directory is called, so it has to survive being one.
# Uppercase is allowed because a run's nonce is base62: 'blazing-ape-0XwE9'.
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
    """Best-effort conversion of user-typed text into a valid name."""
    s = text.strip().lower()
    s = re.sub(r"[^a-z0-9._-]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-._")
    return s[:64]


def generate_workflow_name(taken: Iterable[str] = ()) -> str:
    """An adjective-noun slug, e.g. 'blazing-ape'."""
    import coolname

    taken = set(taken)
    for _ in range(64):
        name = coolname.generate_slug(2)
        if name not in taken:
            return name
    # the two-word space is large but not infinite; widen rather than fail
    for _ in range(64):
        name = coolname.generate_slug(3)
        if name not in taken:
            return name
    raise AssertionError("could not generate an unused workflow name")


# A local agent's home is this machine, which has no alias to be called by.
AGENT_LOCAL_HOST = "local"


def compose_agent_name(prefix: str, host: str) -> str:
    """'blazing', 'sockeye' -> 'blazing-sockeye'."""
    return slugify(f"{prefix}-{host}")


def agent_sort_name(prefix: str, host: str) -> str:
    """The same two words, shuffled so that a list of agents groups by machine.

    Kept as a stored field rather than derived at read time, because only the
    auto-named agents have one: a name someone typed is sorted as it was typed,
    and there is no way to tell the two apart from the string alone.
    """
    return f"{slugify(host)}{slugify(prefix)}"


def generate_agent_name(host: str, taken: Iterable[str] = ()) -> tuple[str, str, str]:
    """A name for an agent on `host`, as (prefix, name, sort_name).

    One word, not two. `generate_slug(2)` is adjective-noun -- 'blazing-ape' --
    and the noun is the half that says nothing once the host is in the name;
    'blazing-ape-sockeye' is a mouthful that identifies no better than
    'blazing-sockeye' does.
    """
    import coolname

    taken = set(taken)
    for _ in range(64):
        prefix = coolname.generate(2)[0]
        name = compose_agent_name(prefix, host)
        if name not in taken and is_valid_name(name):
            return prefix, name, agent_sort_name(prefix, host)
    # one word against one host is a small space; fall back on the pair rather
    # than refuse to make an agent
    for _ in range(64):
        prefix = coolname.generate_slug(2)
        name = compose_agent_name(prefix, host)
        if name not in taken and is_valid_name(name):
            return prefix, name, agent_sort_name(prefix, host)
    raise AssertionError(f"could not generate an unused agent name for host [{host}]")


def generate_run_name(workflow_name: str, taken: Iterable[str] = ()) -> str:
    """'blazing-ape' -> 'blazing-ape-0XwE9'.

    The suffix is a nonce, not a hash. A run is an event: launching the same
    workflow twice must produce two distinguishable runs even though the task
    key -- deliberately -- does not change.
    """
    taken = set(taken)
    suffixes = {n.rsplit("-", 1)[-1] for n in taken}
    suffix = KeyGenerator().GenerateUID(l=RUN_SUFFIX_LENGTH, blacklist=suffixes)
    return f"{workflow_name}-{suffix}"
