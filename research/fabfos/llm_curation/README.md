# LLM curation lanes

An LLM proposes; the element recount decides. That is the whole design, and it is the shape
`curation.py` already uses one layer down — eleven ways of arguing from a name, one way of
deciding. Nothing here trusts a model's account of its own work.

## What runs what

    panel.py      freeze the reactions the lane is measured on   [rdkit-scratch]
    run_panel.py  one prompt revision over one split, billed     [ecspr]
    arbiter.py    recount the elements; append the scoreboard    [rdkit-scratch]
    client.py     the constrained-decoding chat client
    schema.py     the output contract all three agree on

The environment split is forced, not chosen: rdkit is in `rdkit-scratch` and httpx is not,
so the runner writes JSONL and the arbiter reads it rather than calling it. Both need
`PYTHONPATH=src`.

    PYTHONPATH=src mamba run -n rdkit-scratch python research/fabfos/llm_curation/panel.py
    PYTHONPATH=src mamba run -n ecspr        python research/fabfos/llm_curation/run_panel.py \
        --prompt prompts/aam_r1.md --split panel/dev.jsonl --out runs/aam_r1.dev.jsonl
    PYTHONPATH=src mamba run -n rdkit-scratch python research/fabfos/llm_curation/arbiter.py \
        --run runs/aam_r1.dev.jsonl --panel panel/dev.jsonl \
        --scoreboard scoreboard.tsv --note "what changed"

## The two things that make the numbers mean something

**Precision is 100% by construction, so only coverage iterates.** An unbalanced rewrite is
rejected rather than shipped, which means the lane cannot be wrong in the way that matters
and "success rate" can only ever be a coverage figure. Coverage has a real ceiling below
100%: `panel/PANEL.md` names the two strata no rewrite reaches, and refusal is the correct
answer on both.

**Iterate on `dev`, and score `heldout` once.** The split is frozen at a fixed seed for that
reason. A number taken from a split the prompt was tuned against describes the prompt, not
the universe.

Controls are a gate, not a metric: a reaction that banks today must not come back
unbalanced or with different element totals. Zero regressions or the revision does not
count. Over-refusal on a control costs nothing, because in production a banked reaction
never reaches this lane at all.

## Cost

`COSTS.md`, generated from `scoreboard.tsv`. Cost is collected from the first run rather
than retrofitted — a revision that bought coverage by tripling its token spend is a
different trade, and a coverage number alone cannot show it.

## `pilot/`

The two Haiku pilots that established the shape, kept because their failures are what the
current prompts are built against and a summary would lose the cases. `prompt_dir.md` and
`prompt_aam.md` are what was actually sent; the `*_out.jsonl` files are what came back.

Two findings from them are load-bearing and point opposite ways. **AAM failures are
idiosyncratic**, so independent opinions cancel them and an ensemble helps. **Direction
failures are one shared bias** — the model ratifies whichever orientation MetaNetX wrote,
scoring 100% on left-to-right and 15–30% on right-to-left — so an ensemble raises apparent
confidence while leaving accuracy alone. Two orientations of one opinion is worth more
there than three opinions of one orientation.

The third is why `schema.py` has no `balanced` field: self-reported balance was right seven
times and wrong seven times.
