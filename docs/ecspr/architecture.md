# ecspr — architecture

An atom-resolved conductance instrument for metabolic networks. A GPR table says which
reactions a genome can run and how far the evidence backs each; ecspr lays that out as a
network of atoms joined by atom transfers, drives a current through it and reports what
arrived. `ecspr.model` is that measurement, `ecspr.bake` the pipeline that builds the
reference tables it runs on.

## What goes in this file

What ecspr takes in, what it hands back, and how it gets from one to the other in principle.
What is recoverable by reading is not in here — module inventories, flags and command trees
each have their own source of truth, and a transcript of one goes stale without this file
changing.

## The contract

**In**: a baked basis — atom pairs, optionally direction ratios, an element, an orientation
— one or more GPR tables, which are annotation evidence at one row per ORF × channel ×
nominated reaction, and either explicit terminals or a conditions table whose every row
carries its own terminals and its own mask.

**Out**: one long table keyed by condition, probe, orientation, element and readout. `total`
is the measurement, a metabolite readout is what that metabolite drew, and a readout named
with a leading `_` is a per-condition diagnostic — coverage, convergence, abstention — carried
inline so a result cannot be read without what it rests on. Significance is not in it:
`ecspr score` subtracts a named baseline and ranks the difference against a null pool that
`ecspr draw` emits *in the conditions schema*, which is what makes the null arm the same
command with a different file. Both schemas are declared in
`src/metasmith_libraries/data_types/ecspr.yml`.

**ECSPr measures; it does not edit.** There is no weight builder, no metabolite resolver, no
add/delete policy and no background flag, because each is an API for manipulating a network,
and a network manipulation belongs to whoever is designing the experiment. A condition is a
**mask over the rows of a GPR table**: an overexpression is the host row and the clone row
both selected and their conductances summing, a knockout is a drop, and the difference
between two conditions is a subtraction the caller does over the results.

**A probe infers its shape from its inputs**: given terminals it measures the whole table as
one unit and accepts no mask; given a conditions table it measures every row. There is no
mode flag and no batch verb — two entry points into one probe is exactly the drift this
package exists to end.

**A two-point solve already knows its own derivative, so a perturbation sweep is usually the
wrong tool.** Effective conductance is homogeneous of degree one in the conductances, so each
reaction's `dlog C_eff / dlog g_r` equals its share of the dissipated power, which one solve
carries; `reaction_elasticities` returns them and they sum to 1. The consequence for a caller
is that "which reactions does this measurement respond to, and how much" costs one solve rather
than two per reaction, and the answer is a partition rather than a ranking — `1/sum(eps^2)` is
the effective number of reactions a given pair of terminals can respond to at all. Exact on the
symmetric network and first-order under the rectified law; the sum holds either way, by Tellegen.

`--orientation` flips the baked direction reference and nothing else. It cannot be spelled
`--direction`, which is already the *path* to the direction-ratios parquet; the bake's own
identity block calls this field `orientation`.

## How the measurement works

A node is one atom position, `(metabolite, atom rank)`; an edge is a mapped transfer of the
chosen element between two of them, and parallel transfers from different reactions sum onto
one edge. Forward conductance is `E_r * pair_w` — how far the annotation believes the
reaction, times how much of the element moves along that pair — and the backward branch is
`ratio * gp`, the baked thermodynamic asymmetry, so an edge is a rectifier rather than a
resistor. Edges are oriented so `ratio <= 1` always, flipping the edge and inverting the
ratio where it would not be, which is why direction evidence can only throttle a path.

Unit current is injected at the source terminal and drawn at ground, and the probes differ in
what ground is. `two-point` merges the sinks into a single ground and reports the effective
conductance `1/Δφ` alongside what each sink delivered. `ground` hangs a leak edge from every
metabolite onto one universal node, gives the named precursors a port instead of a leak, and
reports the whole per-metabolite draw vector. Hence: naming a metabolite a sink changes the
answer, naming it a readout does not.

The rectifier makes the system nonlinear, so the potentials are not one linear solve but the
minimiser of a convex energy, found by Newton with a line search. The backward branch is
floored strictly positive: a perfect diode leaves the throttled side's potential
unconstrained and the reduced Hessian singular.

**An abundance perturbation cannot express direction.** `gm = ratio * gp` scales both diode
branches together, so the asymmetry is scale-invariant while every readout is non-decreasing
in `E_r` (Rayleigh): direction evidence can mute a false positive but never flip its sign,
and scoring a signed phenotype against a fold-change in `E_r` is unanswerable by construction.
The network is also a steady state with no accumulation variable, so a metabolite *pool* —
the time-integral of net flux — is not a quantity any probe returns.

## Evidence becomes conductance in two stages

**Per-ORF dilution** makes each ORF's total nomination exactly 1.0, spread over the reactions
it nominates, so a promiscuous annotation cannot out-vote a specific one and leave-one-out
stays an exact subtraction. That dilution is one-sided — nothing dilutes a reaction across the
genes nominating it — so **pooling in log-odds** over distinct `(unit, channel, evidence)`
assertions is what additionally stops a paralog family out-voting three independent methods.

**`E_r` is a probability, not a vote count**: repetition saturates, agreement adds, the result
is bounded. The unit sits inside the assertion key deliberately — a physically separate copy
is separate evidence, a paralog within one copy is not. The pre-pooling sum survives as
`belief_mass` because that, not `E_r`, is the additive ledger per-ORF conservation is a
statement about and what a gene *count* has to read.

## Engine and experiment

The split inside `ecspr.model` is what keeps the contract honest. The **engine** measures one
network and sees only a dict of per-reaction conductances, a pair table, a ratio table and
metabolite id strings — never an ORF, a channel, a mask or a condition id. The **experiment
layer** runs it over a set of conditions: it builds the weights, applies the mask, draws a
size-matched null pool *as a conditions table* so the null and the observation traverse the
identical code path, and scores against that pool — the empirical p taken two-sided by
deviation from the null's own centre, since fed and starved are both departures.

## The bake

One artifact in three files — a vocabulary, the atom pairs, the direction ratios — integer-
coded against that vocabulary and sharing a byte-identical identity block. Reaction space is
the whole reaction universe rather than the union of the two source tables, so a reaction with
a direction and no atom map cannot fall out and silently default to reversible. Mixing
versions decodes every node to the wrong metabolite, which is what `assert_same_bake` refuses
and why the three are never repinned one file at a time.

The **AAM lane** adjudicates the whole reaction universe before any mapper runs, then lays
down an additive layer stack — curated maps, an ensemble discounting its two neural members
for shared architectural bias while its combinatorial member votes independently, a curation
sweep, conservation algebra, per-element partial maps — where a layer claims only what the
layers above left unclaimed and the gates refuse rather than warn. The **direction lane** fuses
two correlated thermodynamic estimates with a curated category prior, shrinks a wide posterior
toward reversible, and hands the model `ratio = exp(ΔG'°/RT)`.

**One bake input is produced by the previous bake.** Which reaction the combinatorial mapper
hangs on is not predictable from the molecule, so `aam_forecast` takes it from the last run's
logs. A generation that does not write its own inherits its grandparent's silences, and
nothing says so — the forecast reports a denominator, not an age.

## The seam between the two subpackages

They run at different times, on different hosts, in containers with **disjoint dependency
stacks** — the bake images carry rdkit or a torch stack, the measurement env carries scipy and
cobra — so **neither may import the other at module scope**. That is not a style rule: an
import across the seam does not degrade, it fails at load, inside the one env nothing local
reproduces, six hours into a queued job.

One function reaches across on purpose. `ecspr.bake.encoding.compile_atom_graph` READS a
finished bake and is what the reference gate checks against `ecspr.model.build.graph_from_pairs`
on the string tables, so its `..model.graph` import lives inside the function body, and a test
spawns one interpreter per lane to keep it there.

The bake reaches a job as **one staged input**: `build_references/build.sh` vendors `src/ecspr`
into the transform library as `buildlib::ecspr`, whose `instance_id` is the tree digest — that
digest is what carries provenance from source to baked table, so the vendored copy is
regenerated by the same script that compiles the index, never by hand and never committed.
