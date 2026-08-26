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
nominated reaction, and either explicit terminals or **two tables**: a conditions table
saying what each network *is*, and a tests table saying what is *measured* on it.

**Out**: one long table keyed by test, probe, orientation, element and readout. `total`
is the measurement, a metabolite readout is what that metabolite drew, and a readout named
with a leading `_` is a per-test diagnostic — coverage, convergence, abstention — carried
inline so a result cannot be read without what it rests on. Significance is not in it:
`ecspr score` subtracts the baseline each test names and ranks the difference against a null
pool that `ecspr draw` emits *in the same two schemas*, which is what makes the null arm the
same command with different files. Every schema is declared in
`src/metasmith_libraries/data_types/ecspr.yml`.

**ECSPr measures; it does not edit.** There is no weight builder, no metabolite resolver, no
add/delete policy and no background flag, because each is an API for manipulating a network,
and a network manipulation belongs to whoever is designing the experiment.

**A condition is an ordered set algebra over GPR rows.** Each step is a predicate — this
table, this column, these values — and the condition is the left fold of its steps in `step`
order under `union`, `intersect` and `subtract`. Order decides the answer, which is why
`step` is a column rather than a file position: a subtract before the union that would have
added the rows is a no-op, and the two read identically in a diff. An overexpression is a
union onto rows already present, and `factor` is how much that union is worth — DOSAGE IS A
WEIGHT, NOT A COPY, exact rather than approximate because pooling reads `llr` linearly inside
its saturating term. A knockout subtracts on a gene or a `genome_unit`, never on a reaction:
belief conservation is recomputed on the survivors, so removing one of a gene's reactions
pushes its belief onto the siblings, which is a claim about a gene the experiment did not
make.

**Two arms read that one table.** `ecspr ground` and `ecspr two-point` measure atom
conductance; `ecspr fba` carves a stoichiometric frame with the same rows and solves an LP on
what survives. A construction measured both ways is therefore the *same* construction rather
than two hand-written approximations of one. They differ in which tests table they take, and
in what a weight is allowed to mean: an LP has no use for evidence in nats, so `factor` and a
calibrated `llr` reach the flux-balance arm through exactly one route — a belief floor that
can put a reaction INTO the carve. Neither ever moves a bound, because raising a bound that
was not binding is a bit-exact no-op.

**A probe infers its shape from its inputs**: given terminals it measures the whole table as
one network; given the two tables it measures every test. There is no mode flag and no batch
verb — two entry points into one probe is exactly the drift this package exists to end.

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

## Two levels of separateness, and they are not interchangeable

A **`genome_unit`** is one physically separate copy of DNA inside ONE CELL — a chromosome, a
plasmid, a fosmid insert, an ASKA clone. Two units are one cell: their evidence ADDS, on one
graph. The producer declares it, and nothing infers it from a file name or from `source`,
which on a pipeline-written table is the engine's instance id.

A **member** is one CELL. Two members are two cells, so each gets a PRIVATE copy of every
metabolite and a molecule made by one reaches the other only across a declared bridge. A
community is a compositions table naming members and the metabolites that join them, and a
member is a *condition_id* — which is what lets a member be perturbed, so a strain and a
knockout of it are two members built by one code path.

Putting the unit in the assertion key is not enough on its own to make a second copy count:
the nomination dedup runs first and deletes the second copy's row outright, so the ORF is
namespaced by its unit before any of it. Measured on a toy frame, `E` is 0.397797066112 for
one copy, *the same* 0.397797066112 for two units without that namespacing, and
0.897586872438 with it.

## Engine and experiment

The split inside `ecspr.model` is what keeps the contract honest. The **engine** measures one
network and sees only a dict of per-reaction conductances, a pair table, a ratio table and
metabolite id strings — never an ORF, a channel, a condition or a test id. The **experiment
layer** runs it over a set of tests: it resolves each condition to a weighted row set, draws a
size-matched null pool *as the same two tables* so the null and the observation traverse the
identical code path, and scores against that pool — the empirical p taken two-sided by
deviation from the null's own centre, since fed and starved are both departures.

A composed community stays inside that split. Private copies are bought by PREFIXING the
metabolite ids and a bridge is one more pair row with a pseudo-reaction id, so nothing in the
graph builder, the solver or either measurement transform learns that communities exist — the
same indifference that lets those transforms treat a knockout and a null draw as "another GPR
table". The one cost is that a composed pair table is **not** network-agnostic the way the
reference bake is, so it is staged per network and carries its own type.

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
layers above left unclaimed and the gates refuse rather than warn.

Three rules hold the stack's arithmetic to what a mapper actually said. Each one was absent
for at least two generations, and each cost the atom lane real edges while it was.

- **A member must be a bijection before it may vote.** An atom-map number names one
  correspondence. A member that reuses one sends a single substrate atom to two molecules,
  which is a transfer no chemistry performed. The extractor drops the contradicted atoms and
  keeps the rest of the reaction, so a member abstains where it argues with itself.
- **A participant written `c` times may hold `c` destinations.** Its copies share one atom
  identity, so keeping a single destination dropped a real edge wherever `2 A = B + C` sent
  them to different products. The weight divides across the destinations rather than
  doubling. `graph_from_pairs` folds it into conductance on the strength of its summing to
  one.
- **A multi-component participant is one metabolite and several templates.** MetaNetX writes
  a salt as one `.`-joined SMILES, and the submission builder joins participants the same
  way, so the mapper returns more templates than the reaction has participants. Merging them
  back before the count guard runs is exact rather than approximate: a canonical labelling
  returns the metabolite's own atom ranks whatever order its components arrive in.

The **direction lane** fuses
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

**CAUTION** — the deployed generation breaks that chain. `aam_redox` cannot run under the
engine, so r11 was assembled by `research/fabfos/bake_r11/` over the previous generation's
mapper caches. Its provenance is the commit that pins the script beside the data, not a run's
lineage. Everything it changed is downstream of the mappers, so no mapper ran and `aam_cache/`
and `logs/` are the previous generation's bytes. Restore the chain before treating a lineage
query against this chunk as authoritative.
