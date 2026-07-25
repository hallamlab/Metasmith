"""Readable names for workflows and runs.

Task keys are content-derived and stable, which is what makes them useful for
caching and useless for conversation. These are the names people actually say
out loud: a workflow is "blazing-ape", and each of its runs is "blazing-ape-0XwE9".
The key is still the identity; the name is the handle.
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
