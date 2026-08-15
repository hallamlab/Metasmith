# The upstream ASPIRE pipeline, vendored

`ASPIRE/` is a verbatim copy of the upstream Nextflow pipeline that
`src/metasmith_libraries/transforms/aspire/` is a port of. It is somebody else's
code and nothing here modifies it — every file is exactly what upstream committed.

    source     projects/metasmith-libraries/aspire/scratch/ASPIRE (a git clone)
    commit     50a18c57a00dd840848499b7ccec975d5ac70af4
    dated      2026-07-17, "Group SI accumulation plots by depth"
    contents   149 entries (125 files + 24 symlinks), 51 commits of history
    scale      asv_pipeline.nf is 6,258 lines wiring 45 processes

## Why it is here at all

It was tracked in no repository that anyone could reach. The clone declared two
GitHub remotes —

    origin     git@github.com:phy0x1a79ed/ASPIRE.git
    upstream   git@github.com:Tony-xy-Liu/ASPIRE.git

— and on 2026-08-15 **both answered `Repository not found`**, over SSH and again
through a `repo`-scoped `gh` token that authenticates fine against GitHub as
`phy0x1a79ed`. Deleted, renamed, or never shared with this account; from here they
are indistinguishable and equally useless. The clone sat inside a directory the
library repo git-ignores, so deleting that worktree — which this migration does —
would have destroyed the only copy.

The transform port cites this tree **by file and line** (`asv_pipeline.nf:3470` and
forty-odd others). Losing it does not merely lose the pipeline; it silently voids
every citation in `transforms/aspire/`, and nothing would announce that.

## What is preserved where

The 51 commits do not survive as history — the monorepo shares no object store with
that clone, and this is a snapshot. They survive as a bundle:

    data/archive/repo-bundles/ASPIRE-20260815.bundle

which `git bundle verify` reports as recording a complete history, and which also
carries `origin/schen455-patch-1` (`dd2eae5`), a branch reachable from no other ref.
That chunk is the same one T0 pinned the three source-repo bundles into, so the
workspace's off-site cache push covers it.

If upstream ever reappears, reconcile against the bundle rather than against this
directory: this directory is a snapshot with no history to compare.

## Two properties worth knowing before you touch it

The 24 symlinks are load-bearing. `processes/*/env.yml` are relative links into
`processes/shared_envs/`, which is how one conda environment declaration serves
several processes. They are relative and entirely internal, so the tree is portable,
but a copy that dereferences them (`cp -rL`, some archive tools) turns one shared
declaration into twenty-four independently-drifting ones. Copy with `git archive` or
`cp -a`.

`ASPIRE/.gitignore` is upstream's and stays upstream's. It ignores `reports/`,
`work/`, `env/` and other broad names, scoped to this subtree — faithful to what
upstream tracks, and worth remembering if you ever wonder why a file you put in
there did not appear in `git status`.
