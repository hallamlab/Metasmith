"""Shell for fetching a reference database into a transform's work directory."""

from __future__ import annotations

import shlex
from pathlib import Path


# Inactivity, not total transfer time: a 5 GB archive is minutes of legitimate
# silence, but a server that has stopped sending is not going to start again.
_STALL_TIMEOUT_S = 120
_TRIES = 3


def FetchCommand(
    urls: "str | list[str]",
    dest: "str | Path",
    *,
    tries: int = _TRIES,
    timeout_s: int = _STALL_TIMEOUT_S,
    label: str | None = None,
) -> str:
    """Shell that puts `dest` in place from the first of `urls` that answers.

    Three properties the database transforms need and a bare `wget -q` does not
    have.

    It **narrates**: `--progress=dot:giga` puts a line in `.command.log` for
    every gigabyte, so a seventeen-minute fetch is visibly a fetch rather than
    a hang.

    It **gives up**: a stalled transfer is abandoned after `timeout_s` of
    silence and retried at most `tries` times, instead of being waited on
    forever.

    It **resumes**: the transfer lands on `<dest>.part` and is promoted to
    `dest` only once wget reports it complete, so `dest` existing means a whole
    archive and a second attempt in the same work directory fetches nothing.
    A partial `.part` continues from where it stopped. That matters because the
    fetch and the indexing that follows it are one transform and so one cache
    entry: a failure in the indexing discards the fetch, and the reporter paid
    for the same 5.5 GB three times over.
    """
    if isinstance(urls, (str, Path)):
        urls = [str(urls)]
    dest = str(dest)
    part = f"{dest}.part"
    what = label or Path(dest).name
    flags = (
        f"--continue --progress=dot:giga "
        f"--timeout={timeout_s} --read-timeout={timeout_s} --tries={tries}"
    )
    attempts = [f"wget {flags} {shlex.quote(str(u))} -O {part}" for u in urls]
    fail = (
        f'{{ echo "every source for {what} failed" >&2; exit 1; }}'
    )
    return "\n".join([
        f'if [ -s {dest} ]; then',
        f'    echo "{what} is already here; not fetching it again"',
        f'else',
        *[f"    {a} || \\" for a in attempts],
        f"    {fail}",
        f"    mv {part} {dest}",
        f'fi',
    ])
