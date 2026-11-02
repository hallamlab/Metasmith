from __future__ import annotations

from dataclasses import dataclass

from ..logging import Log


NO_ROOM_ADVICE = (
    "the agent home's filesystem has no room for another file; free files under it,"
    " or ask the cluster admins for a higher file limit"
)

# Measured on fir: 23 cache hits added about 31,000 files, and a step that runs fresh
# writes its work dir and outputs on top of that. Per step of the plan, not per task:
# a step's task count is not known before nextflow expands it, so this is a floor.
FILES_PER_STEP = 2_000
FILES_FLOOR = 50_000
SPACE_FLOOR_SHARE = 0.05

_ERRNO_NAMES = {
    "quota exceeded": "EDQUOT",
    "No space left": "ENOSPC",
    "Permission denied": "EACCES",
    "Read-only file system": "EROFS",
}


class HomeHeadroomError(Exception):
    pass


@dataclass
class Quota:
    source: str
    files_used: int
    files_limit: int
    kbytes_used: int
    kbytes_limit: int

    @property
    def files_free(self) -> int | None:
        return self.files_limit - self.files_used if self.files_limit else None

    @property
    def kbytes_free(self) -> int | None:
        return self.kbytes_limit - self.kbytes_used if self.kbytes_limit else None


def render_probe(directory) -> str:
    # `lfs quota -u` reports no limit on a cluster that charges a directory's Lustre
    # project id instead, as fir does for /scratch, so all three identities are asked.
    return "\n".join([
        "(",
        f'_msm_d="{directory}"',
        '_msm_t="$_msm_d/.msm_headroom.$$"',
        'if _msm_e=$(mkdir "$_msm_t" 2>&1); then',
        '    if _msm_e=$( { : > "$_msm_t/probe"; } 2>&1 ); then echo "CREATE|ok"',
        '    else echo "CREATE|fail|$_msm_t/probe|${_msm_e##*: }"; fi',
        '    rm -f "$_msm_t/probe"; rmdir "$_msm_t"',
        'else echo "CREATE|fail|$_msm_t|${_msm_e##*: }"; fi',
        '_msm_m=$(df -P "$_msm_d" 2>/dev/null | awk \'NR>1{print $NF}\' | tail -n1)',
        'df -Pk "$_msm_d" 2>/dev/null | awk \'NR>1{l=$0} END{n=split(l,a," "); if(n>=6) print "DFK|"a[n-4]"|"a[n-3]}\'',
        'df -Pi "$_msm_d" 2>/dev/null | awk \'NR>1{l=$0} END{n=split(l,a," "); if(n>=6) print "DFI|"a[n-4]"|"a[n-3]}\'',
        'if [ -n "$_msm_m" ] && command -v lfs >/dev/null 2>&1; then',
        '    _msm_T=$(command -v timeout >/dev/null && echo "timeout 20")',
        '    _msm_p=$(lfs project -d "$_msm_d" 2>/dev/null | awk \'{print $1}\')',
        '    for _msm_q in "u $(id -un)" "g $(stat -c %G "$_msm_d" 2>/dev/null)" "p $_msm_p"; do',
        '        set -- $_msm_q',
        '        [ -n "$2" ] && [ "$2" != 0 ] || continue',
        '        echo "LFS|$1 $2|$_msm_m|$($_msm_T lfs quota -q -$1 "$2" "$_msm_m" 2>/dev/null | tr \'\\n\' \' \')"',
        '    done',
        'fi',
        ")",
    ])


def _int(token: str) -> int | None:
    token = token.rstrip("*")
    return int(token) if token.isdigit() else None


def _parse_lfs(source: str, mount: str, text: str) -> Quota | None:
    tokens = text.split()
    if mount not in tokens: return None
    fields = tokens[tokens.index(mount)+1:]
    if len(fields) < 7: return None
    kb_used, kb_soft, kb_hard = (_int(x) for x in fields[0:3])
    f_used, f_soft, f_hard = (_int(x) for x in fields[4:7])
    if None in (kb_used, kb_soft, kb_hard, f_used, f_soft, f_hard): return None
    return Quota(
        source=f"lfs quota -{source} on {mount}",
        files_used=f_used, files_limit=f_hard or f_soft,
        kbytes_used=kb_used, kbytes_limit=kb_hard or kb_soft,
    )


def errno_name(reason: str) -> str:
    for needle, name in _ERRNO_NAMES.items():
        if needle in reason:
            return f"{reason} ({name})"
    return reason


def parse_probe(lines: list[str]) -> tuple[bool, tuple[str, str] | None, list[Quota]]:
    refused: tuple[str, str] | None = None
    created = False
    quotas: list[Quota] = []
    df_kb: tuple[int, int] | None = None
    for line in lines:
        parts = line.strip().split("|")
        match parts[0]:
            case "CREATE" if len(parts) >= 2:
                if parts[1] == "ok":
                    created = True
                elif len(parts) >= 4:
                    refused = (parts[2], "|".join(parts[3:]).strip() or "unknown error")
            case "LFS" if len(parts) >= 4:
                q = _parse_lfs(parts[1], parts[2], "|".join(parts[3:]))
                if q is not None: quotas.append(q)
            case "DFK" if len(parts) == 3 and _int(parts[1]) is not None and _int(parts[2]) is not None:
                df_kb = (_int(parts[1]), _int(parts[2]))
            case "DFI" if len(parts) == 3 and _int(parts[1]) is not None and _int(parts[2]) is not None:
                total, used = _int(parts[1]), _int(parts[2])
                kb_total, kb_used = df_kb or (0, 0)
                quotas.append(Quota("df on the agent home", used, total, kb_used, kb_total))
    return created or refused is not None, refused, quotas


def _fmt_kb(kb: int) -> str:
    return f"{kb/1024**3:.1f} TiB" if kb >= 1024**3 else f"{kb/1024**2:.1f} GiB"


def check_headroom(lines: list[str], steps: int, task_key: str) -> list[str]:
    answered, refused, quotas = parse_probe(lines)
    if not answered:
        Log.Warn("could not check that the agent home can create files; launching anyway")
        return []
    # Lustre's statfs answers a project-quota directory with the quota itself, so
    # df and `lfs quota -p` can report one limit twice.
    limited: dict[tuple[int, int], Quota] = {}
    for q in sorted(quotas, key=lambda q: not q.source.startswith("lfs")):
        if q.files_limit or q.kbytes_limit:
            limited.setdefault((q.files_used, q.files_limit), q)
    limited = list(limited.values())
    numbers = "; ".join(
        f"{q.source}: {q.files_used:,} of {q.files_limit:,} files"
        + (f", {_fmt_kb(q.kbytes_used)} of {_fmt_kb(q.kbytes_limit)}" if q.kbytes_limit else "")
        for q in limited
    )
    if refused is not None:
        path, reason = refused
        raise HomeHeadroomError(
            f"refusing to launch [{task_key}]: cannot create [{path}]: {errno_name(reason)}."
            + (f" Usage: {numbers}." if numbers else "")
            + f" If this is a quota or a full disk, {NO_ROOM_ADVICE}."
        )
    wanted = max(FILES_FLOOR, FILES_PER_STEP*steps)
    warnings: list[str] = []
    for q in limited:
        if q.files_free is not None and q.files_free < wanted:
            warnings.append(
                f"[{q.files_free:,}] files left under {q.source} ({q.files_used:,} of"
                f" {q.files_limit:,} used); a [{steps}]-step run plausibly creates [{wanted:,}] or"
                f" more, counting the work dirs and cache-hit link trees in nxf_work. A run"
                f" that reaches the limit dies mid-step. To avoid it, free files under the"
                f" agent home, or ask the cluster admins for a higher file limit"
            )
        if q.kbytes_free is not None and q.kbytes_free < q.kbytes_limit*SPACE_FLOOR_SHARE:
            warnings.append(
                f"[{_fmt_kb(max(0, q.kbytes_free))}] of space left under {q.source}"
                f" ({_fmt_kb(q.kbytes_used)} of {_fmt_kb(q.kbytes_limit)} used). Free space"
                f" under the agent home, or ask the cluster admins for a higher limit"
            )
    for w in warnings:
        Log.Warn(w)
    return warnings
