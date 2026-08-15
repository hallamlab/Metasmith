# ecspr — architecture

An atom-resolved conductance instrument for metabolic networks: a GPR table in, a measurement
out. Extracted from fabfos, and the only transform in the tree whose protocol is an **algorithm**
rather than a dispatch into somebody else's tool — which is why it is a package with a command
line instead of a staged resource file.

## Why it is its own module

`env::ecspr.env` declares it like any other tool, so the fabfos transform and a bare benchmark
run **the same executable** and cannot drift into two call sequences. That is the whole reason
for the shape: a method small enough to stage as a file does not need this, and one this size
would otherwise grow a second, subtly different implementation on the benchmark side.

It builds as its own project, so `pip install -e src/ecspr` is the entire dev loop and the
container can install it without the fabfos wheel. `dev/ecspr.sh` covers install, run, test,
build and upload.

## The contract

**ECSPr measures; it does not edit.** There is no weight builder, no metabolite resolver, no
add/delete policy and no background flag, because each of those is an API for manipulating a
network, and a network manipulation belongs to whoever is designing the experiment. A condition
is a **mask over the rows of a GPR table**, and the difference between two conditions is a
subtraction the caller does over the results.

Two layers, and the split is the reason the contract holds. The **engine** measures one network
(the rectified-diode Newton solve and its CHOLMOD binding, the atom network and its terminals,
and the builder that turns atom pairs × per-reaction weights × direction ratios into a network).
The **experiment layer** runs the engine over a set of conditions — belief-conservation weights,
the mask, the conditions table, the probes, a null pool drawn *as* a conditions table, and
scoring against that pool.

**A probe infers its shape from its inputs**: given terminals it measures the whole table as one
unit and accepts no mask; given a conditions table it measures every row, each carrying its own
terminals and mask. There is no mode flag and no batch verb — two entry points into one probe is
exactly the drift this package exists to end.

`--orientation` flips the baked direction reference and nothing else. It cannot be spelled
`--direction`, which is already the *path* to the direction-ratios parquet; the bake's own
identity block calls this field `orientation`.

## Dependencies are declared once

`env.yml` is the single dependency spec, feeding three consumers that would otherwise drift: the
dev conda env, the conda recipe, and the container image. `pyproject.toml` deliberately lists
none — a pip metadata copy would be a second place for them to be wrong, and the package is
installed `--no-deps` into an env conda already solved.

**`numpy<2` is not a preference.** scikit-sparse's CHOLMOD binding and cobra jointly require it,
which is also why the fabfos image splits into two envs (polars needs `numpy>=2`). conda-forge's
suitesparse is the reliable route to CHOLMOD on this class of host; pip is not. cobra is imported
lazily and sksparse is guarded, so both are honest runtime deps rather than import-time ones —
but a build that resolves without them **silently drops to the splu fallback**, which is why they
are declared anyway.

`build_hash.txt` is gitignored but MUST ship as package data: an installed ecspr reads it to
report its full version, so leaving it out makes every container claim the bare release segment
and a result stops being traceable to a source state.
