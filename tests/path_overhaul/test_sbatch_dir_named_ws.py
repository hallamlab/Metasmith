"""`bin/sbatch:36,40` uses `r"/\\w*/nxf_work/.*"` to extract the path
tail starting from the run key. The regex returns the *first* leftmost
match, which silently picks the wrong segment when any directory
between the run key and `nxf_work/` happens to look like a word.

The canonical pathological case: a sample / sub-workdir literally named
`ws` (the same name the in-container workdir is bound under). With cwd
`/host/runs/<KEY>/ws/nxf_work/<hash>`, the regex extracts
`ws/nxf_work/<hash>` and the downstream
`external_workspace = staged_dir / tail.split('/nxf_work/')[0]`
produces `staged_dir/ws` instead of `staged_dir/<KEY>`.

This test pins the bug; a `PathMap.FromExternalCwd` replacement will
identify the run key by ancestry (the directory whose name matches the
task key passed to the agent), not by regex on the segment shape.

Reference: audit Category B (regex parses).
"""
import re


def _sbatch_derive_tail(cwd_str: str) -> str:
    """Replicates `src/metasmith/bin/sbatch:36,40`.

    The actual sbatch script wraps this in a branching `if` on whether
    cwd starts with `/ws`, but the regex branch is what we are pinning.
    """
    return next(re.finditer(r"/\w*/nxf_work/.*", cwd_str)).group()[1:]


def test_regex_picks_wrong_segment_when_sample_dir_named_ws() -> None:
    """sbatch run from a host cwd where a sub-directory between the run
    key and `nxf_work/` is named `ws`. The first `/\\w*/nxf_work/` match
    grabs the `ws` segment and the run key is lost.
    """
    run_key = "REALKEY"
    cwd_str = f"/host/agent/runs/{run_key}/ws/nxf_work/aa/bb"

    tail = _sbatch_derive_tail(cwd_str)

    # The tail must start with the run key so that
    # `staged_dir / tail.split('/nxf_work/')[0]` resolves to the actual
    # external workspace, not the inner `ws` directory.
    head = tail.split("/nxf_work/")[0]
    assert head == run_key, (
        f"sbatch regex extracted wrong head: got [{head}], expected [{run_key}]. "
        f"Full tail=[{tail}], cwd=[{cwd_str}]"
    )


def test_regex_picks_wrong_segment_with_extra_subdir() -> None:
    """A second shape: any extra segment between the run key and
    `nxf_work/` triggers the same first-match-wins bug.
    """
    run_key = "REALKEY"
    cwd_str = f"/scratch/site/runs/{run_key}/sample_a/nxf_work/aa/bb"

    tail = _sbatch_derive_tail(cwd_str)

    head = tail.split("/nxf_work/")[0]
    assert head == run_key, (
        f"sbatch regex extracted wrong head: got [{head}], expected [{run_key}]. "
        f"Full tail=[{tail}], cwd=[{cwd_str}]"
    )
