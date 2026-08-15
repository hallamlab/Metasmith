# Concept figure: the reaction network, laid out by true pairwise ECSPr I_eff distance

## What goes in this file

Why this pipeline has the shape it does, which inputs it depends on that live outside the
repo, and the measurements that closed off the approaches it does not take. Per-flag
behaviour is in each script's `--help` and module docstring; do not transcribe it here.

## The layout input, and why it changed three times (and did not change a fourth)

Each version fixed a defect in the last:

1. **Shared-metabolite jaccard** — ubiquitous cofactors dominated every neighbourhood.
2. **Conductance-weighted bipartite ("star") graph** — a path could enter and leave a
   reaction through two *substrates*, which is mass flow that cannot happen. ECSPr retired
   this topology; a zero-carbon channel scored 21x a real one on MNXR106432.
3. **Landmark profile on the atom graph** (`network_ground_landmark`) — each reaction
   described by the currents it draws from 296 probe sources, compared by cosine. Correct
   physics, wrong relation: it places reactions together when they *behave* alike, so two
   analogous reactions in unrelated pathways look identical.
4. **True pairwise I_eff distance** (`network_ieff_exact_*`, this pipeline) — entry (a,b)
   is the current b draws when one ampere is injected at a's product atoms with the OMEGA
   universal leak grounded, solved on the rectified network. Reactions sit together because
   current actually flows between them.

The step from 3 to 4 is not a refinement. On the same 3,327 reactions, top-10 neighbour
overlap between the landmark similarity and the exact distance is **0.001–0.002 against a
random baseline of 0.003** — robust across cosine/correlation/Euclidean on log-current and
across 16 to 3,327 landmark columns. The two relations share essentially no neighbour
structure. The exact distance also holds KEGG pathways together better (mean scatter index
0.80 vs 0.93, 1.0 = randomly placed).

### The elemental landmark profile (`ieff_landmark`), and what it buys

Approach 3 is nonetheless the only thing that reaches the universe, so it exists here in a
deliberately small form: four landmarks, one per biomass element — oxaloacetate (C),
L-glutamate (N), phosphate (P), L-cysteine (S) — each solved on **its own element's** atom
graph in both directions, weighted C:N:P:S at 100:10:1:1. Eight solves, eight numbers per
reaction, cosine into UMAP. It is a behavioural profile and inherits the refutation above;
nothing it produces may be read as "these reactions exchange mass".

What it costs and what it is worth, measured: 57,637 universe reactions in **550 s of solves
plus 50 s of UMAP**, against 332–1,175 h for the exact sweep. Mean KEGG scatter index 0.798
at universe scale; 0.707 on the EPI300 GEM lane where the exact metric scores **0.445**. So
it is well short of the exact relation and well short of random, on the set that has no
alternative.

Three things the 8 columns turned out to be. Their participation ratio is **2.14** — the
matrix is two-dimensional in all but name, which is why the layouts are hollow shells. The
reverse solve is near-redundant except on carbon (in/out correlate 0.99 N, 0.99 P, 1.00 S,
0.77 C), because only the carbon graph is big enough for the diode signs to matter. And
despite P and S being 0.8% of the cosine inner product, dropping them is what breaks the
figure — they are the axes orthogonal to carbon. Column subsets, EPI300 GEM lane, one regime:
all 8 = 0.707, drop P/S reverse = 0.762, forward only = 0.776, C_out+C_in+N_out = **0.888**
with TCA worse than random. Magnitude share is not information share; keep all eight.

The biomass ratio is applied as `sqrt(w)` per block, so each element's contribution to the
cosine is proportional to `w`. Realised shares are C 92.1 / N 7.1 / P 0.67 / S 0.14 percent
against the intended 89.3 / 8.9 / 0.89 / 0.89: C, N and P land, and S comes in 6× light
because it reaches only 14–24% of reactions and so has little magnitude to weight.

`--seed 0` drops UMAP's `random_state`, which is what lets it use every core — 57k points go
from minutes to 50 s. The layout is then unreproducible, so the saved `.xy.npy` is the record
of it, and figures meant to be read together must share one.

## Power and potential drop, and why current stays

The one solve already computes node potentials, so `--channels power,dv` stores two more
per-reaction channels for free: dissipated power `|I_e| * |dV_e|`, and each reaction's drop
below the network's peak potential. The motivation was that current is *step-blind* — at
leak 1e-6 the decay length exceeds the graph diameter, so what attenuates is branching, not
distance — while `dV` is step-sensitive, and their product is a step count weighted by mass
flow. Measured on the 1,489-reaction EPI300 GEM lane, mean KEGG scatter index (lower is
better, 1.0 = randomly placed):

| channel | leak 1e-6 | leak 1e-2 |
|---|---|---|
| current (incumbent) | **0.445** | 0.572 |
| power | 0.798 | 0.526 |
| dV | 0.712 | 0.691 |
| dV + 0.5·power | 0.620 | 0.596 |
| dV + 2·power | 0.624 | 0.590 |

**Current at leak 1e-6 wins, and power at that leak is the worst channel tried.** The
mechanism is the reverse of the intended one. Power was supposed to penalise promiscuous
cofactor edges, because `P = I^2/g` and their summed-parallel conductance reaches 2,666.
It does — but `dV = I/g` is *largest* on the weakest edges, so power instead promotes
low-conductance reactions to be everyone's nearest neighbour: 1% of targets absorb **45%**
of all top-30 neighbour slots under power against 13% under current. It swaps a hub penalty
for a hub inversion. `dV` alone does the same thing more mildly (33%).

Two honest asides. Power *is* the better channel once the field is localized — at leak 1e-2
it beats current there (0.526 vs 0.572) and its hub concentration drops to 10% — but raising
the leak costs the incumbent more than power gains, so the best power figure is still worse
than the plain 1e-6 current figure. And the channels are genuinely different measurements,
not re-scalings: power/current top-30 overlap is 0.377, dV/current 0.111.

The channels stay in `ieff_sweep`/`ieff_layout` because they cost nothing to store and this
is how the question gets re-asked at another scale. Nothing shipped uses them.

## Pipeline

`atom_graph` builds the atom-resolved graph and per-reaction terminals -> `ieff_ground`
solves and verifies -> `ieff_sweep` runs one solve per reaction into a resumable top-K
store -> `ieff_layout` symmetrises, builds the kNN graph and renders (`ring_layout` renders the same
graph on a circle instead of the plane) -> `ieff_validate`
checks conservation and compares against the landmark shortcut. `ieff_landmark` is the
eight-solve elemental profile, the only path that reaches the universe. `gpr_ieff` is the host-network
entry point: a single-source probe, and the pack step that turns a store into the delivered
sparse table. Feeding it: `gpr_union` fuses host and insert GPR tables, `gpr_mask` slices one
back out by origin, `dominant_clone` ranks the inserts, and `mask_check` compares a masked
community solve against a standalone one.

Only reactions are laid out: the metric is reaction-to-reaction, and promoting metabolites to
points is what let the star figure spend half its neighbour budget on the wrong node type. The
host figure does *draw* them, at the mean position of the reactions they carry an atom through
— incidence rendered onto the layout, telling you nothing the reactions did not already say.

## The layout regime

There are two regimes here, and which one a figure used is not recoverable from the figure.
**seed 42, 30 neighbours, min_dist 1.2, spread 2.5, radial gamma 0.4** is what a min_dist
sweep on the universe picked by eye and the retired `network_concept.py` carried as its
defaults — but they are *not* `ieff_layout`'s defaults, so a figure drawn without the flags
is at min_dist 0.05 / spread 1.0 / gamma 1.0, a visibly tighter regime. The shipped EPI300
GEM figure is one of those: its argv had no tuning flags at all. State the regime when
citing a figure; it is in each layout's `--metrics-out` argv from here on.

The tuned regime is not the better one, and is not general. It exists to force apart the
landmark profile, whose participation ratio of 2.14 collapses without it; applied to the exact
metric it inflates a layout that already has structure — same matrix, same seed, mean KEGG
scatter **0.449 at the defaults against 0.582 at min_dist 1.2**. Use it only for landmark
figures, or when a figure has to share geometry with one.

`spread` is UMAP's own (and must not fall below `min_dist`); the gamma is not a UMAP
parameter at all but a post-hoc rescaling of the returned radii about the centroid, and
`network_concept.compress_radial_outliers` remains its definition rather than being restated
in the layout script.

The percentile view clip is not the place to look for the gamma's effect: a symmetric
percentile band clips the same count whatever the distribution, and it does — 92 of 1,489
points off-frame on the EPI300 network both before and after. The radial spread is what
moves, p99/median radius 2.48 -> 1.37. And a fresh layout breaks any shared geometry: figures
meant to be read side by side must share a coordinate file (`--xy` with `--reuse-xy`), and
re-running one at new settings silently stops being comparable unless the other is re-run too.

## Scales

| scale | reactions | atom nodes | exact rectified sweep |
|---|---|---|---|
| `medium` | 3,327 | 29,706 | 20 min, 4 workers |
| `kegg` | 12,708 | 136,159 | **2.40 h, 12 workers** — the shipped figure |
| host + 183 inserts | 9,572 | 86,200 | **18.5 min, 120 workers on fir** |
| `universe` | 57,637 | 589,257 | 332–1,175 h at 6 workers — **does not fit**; `ieff_landmark` covers it in 10 min |

`kegg` is the star medium plus every MetaNetX reaction carrying a `kegg.reaction:`
cross-reference: the largest scale an exact sweep fits into a working day, and the set the
pathway overlay can actually speak about. It ran to 12,708/12,708 rows with zero solver
refusals and zero fallbacks, per-source median 4.8–6.8 s across the twelve workers.

`network_ieff_exact_kegg` is the deliverable; `network_ieff_exact_medium` is the same
pipeline at the smaller scale, kept because the dense matrix it also writes is what the
exact-versus-landmark and min_K validation needs.

Pathways hold together far better at `kegg` than at `medium` — mean scatter index 0.36
against 0.80, best 0.17 (valine/leucine/isoleucine degradation), worst 0.51 (porphyrin).
Read that as the figure getting better with coverage, not as a like-for-like measurement:
scatter index is normalised against random sets *within one layout*, so it is comparable
between two layouts of the same points and only suggestive across scales.

## Why the universe sweep is out, and what was tried

Measured on the fully optimized exact solver, seven universe sources: cold starts 438.9 s /
441.8 s / 482.5 s at 19–20 Newton iterations; warm starts down a contiguous proximity run
223.4 s and 298.1 s at 9 and 12 iterations. Six workers is the ceiling memory allows (a
universe worker peaks near 7 GB of 58). That is **332 h warm to 1,175 h cold** against an
eight-hour budget — short by 40x to 150x, not by a factor a better constant recovers. The
warm figure is itself optimistic: it is pulled down by reactions that share their product
atom set with the previous source and so resolve in one iteration (8.5% of sources at
medium scale).

The rectified conductance depends on the sign of each edge's own potential drop, so the
matrix changes between Newton iterates and every iterate pays a fresh numeric
factorization: **30 s against a 0.11 s triangular solve** at 589k unknowns. That ratio, not
the solver, is the whole problem — with a *fixed* matrix the universe sweeps in about an
hour.

CHOLMOD is not the bottleneck and is not silently punting: over 54 universe solves it
returned worst relative residual 1.6e-14 with zero fallbacks. A "faster precise solver"
does not exist here because a direct factorization is already the right answer for many
right-hand sides; near-linear-time Laplacian solvers win on one solve and lose badly once
thousands amortize a factorization.

Four ways to make the matrix cheaper or constant were measured and each failed:

* **Fill-reducing ordering.** default 12.9 s / AMD 24.3 s / METIS 16.0 s / NESDIS 15.1 s /
  best 15.4 s per refactorization. Nested dissection finds no exploitable separator, so the
  static hub-decomposition idea is already tested. Do not spend time here again.
* **Per-source flow cone** (goal-directed search, the live half of the Google-Maps
  analogy). Dead on arrival: 99.9% of a source's flow needs 92% of edges, and the cone at
  cover 0.9999 keeps 98% of them. The leak does not localize it either — the field is
  identical from leak 1e-6 to 1e-4 because the leak is negligible against edge conductance,
  so the decay length exceeds the graph diameter.
* **Inexact Newton with a stale factor as PCG preconditioner.** Answer-identical (top-10
  1.0000, rho 1.000000 against the exact reference) but does not converge: 35% of iterates
  hit the iteration cap, because `d` moves across the 9 orders the diode floor allows.
  Worth at most 2x, not the 67x needed.
* **Frozen consensus active set.** 83% of edges keep the same diode branch across every
  source, so freezing the sign pattern would make the matrix constant. Measured against the
  exact reference: top-10 overlap 0.69–0.77, no better than the plain symmetric field. The
  per-source active set is load-bearing.

Contraction hierarchies and transit-node routing do have exact electrical twins — Kron
reduction and the Schur complement onto a separator — but those *are* Gaussian elimination
on the Laplacian, which CHOLMOD already performs, and the ordering measurement says they
have nothing left to give.

What did work is unglamorous: proximity source ordering with warm starts (Newton iterations
26–36 -> 3–11), a fixed-pattern Laplacian assembler (10.6x over the sparse triple product,
bit-exact), and **one thread per worker** — CHOLMOD's internal threading scales so poorly
here that 12 single-threaded workers beat 5 multithreaded ones by 4.3x.

## min_K, and why it helps storage but not compute

"Only the main routes matter" is true of the *answer* and false of the *computation*: every
cheap way of not computing the tail changed the neighbours (the cone and frozen-active-set
measurements above). The sweep therefore solves exactly and truncates on write.

Rows are fully dense (smallest attributed current measured: 7e-15), so the store keeps
top-K per row plus the row sum. At `kegg`, K=2,000 of 12,708 targets carries a median 71% of
a row's current mass (p1 59%, min 54%); on the 9,572-reaction community network the same K
carries a median 74% (min 60%) — and the layout only ever reads the top 30, so the
truncation is nowhere near binding.

The stored `rowsum` is **not** a conservation quantity, despite looking like one. Attribution
credits the same ampere to every reaction it flows through, so a row totals roughly the
number of reactions a unit of current traverses before it reaches ground: median 10.4 A at
`kegg`, range 0.05–38. Conservation is the separate OMEGA test — current into ground
1.000000000 A against the ampere injected, KCL residual ~1e-7, and `bincount` attribution
agreeing with the `reaction_currents` formula to 1e-16.

## Host networks, and where the current actually is

The scales above are universe slices, which is deliberately not a claim about any organism.
Pointing the same instrument at a GPR table makes it one: the medium becomes the reaction set
a host's called genes can run. E. coli EPI300's curated-GEM lane gives 2,290 MNXR, of which
1,509 carry carbon atom-pair rows and the giant component keeps **1,489 reactions over 16,539
atom nodes** — minutes, not the hours the universe cannot afford. The reaction set carries the
organism claim on its own, so the graph is unweighted; folding in the per-call `raw_score`
would additionally encode annotation confidence, and the two lanes do not score on one scale
(`gpr_gem` is all 1.0 by construction, `gpr_denovo` spans 0.01 to 2293).

**99% of the current is not a sparsifying level.** Measured on this network with one ampere
injected at D-glucose's six carbons: 99% of the current needs 88.3% of metabolites, 85.0% of
reactions and 70.0% of atom-transfer edges. The concentration is an order of magnitude lower
down — 50% of the current sits in 5.6% of metabolites, 7.9% of reactions, 3.3% of edges. So
the delivered table truncates each row at 50% of its current mass, which keeps a median 100
partners of 1,489 (density 0.073) and reproduces the untruncated matrix's top-10 neighbours at
0.994 and top-30 at 0.993 — the same conclusion the universe-scale min_K section reached, now
measured on the network being shipped rather than carried across from it.

That does not conflict with the cone measurement above: concentration in the reactions a
current visits and concentration in the edges it uses are different statements, and only the
first is what a pairwise row stores.

The **drain** reading — what each metabolite dumps to OMEGA, which sums to exactly the ampere
injected and so looks like the natural share — is uninformative and should not be reported as
a result. `attach_leak` gives every metabolite the same *total* conductance to ground split
across its atoms, and at 1e-6 that is negligible against edge conductance, so the potential
field is flat and every metabolite drains about 1/N (measured std/mean 0.072). It is the same
effect as the field being identical from leak 1e-6 to 1e-4. Transit and attributed reaction
current are the readouts that carry information.

## The host plus its clones, and why a mask is not a measurement

**Every count in this section is on the superseded 183-insert set.** The recovered set has
since gone to 170 and the GPR tables were retargeted onto it; this lane has not been
re-solved, so these numbers describe the 2026-07-27 tables, not the ones in the tree.

FabFos exists to say what cloned environmental DNA adds to a host, so the same instrument is
pointed at the EPI300 GEM lane unioned with all 183 SCADC fosmid inserts. `gpr_union` builds
that one table — the sweep and the layout both read a single `mnxr` column, so a union table
beats a second code path — and carries an `origin` column naming the host or the insert each
row came from. Every later slice is a mask on that column. 14,097 reactions collapse to a
**9,572-reaction giant component over 86,200 atom nodes**, which is the row in the scales
table above. It ran to 9,572/9,572 rows with zero refusals in all 120 blocks, per-source
median 5.7–12.8 s across the workers.

The clone drawn against the host is chosen by measurement, not by name: `dominant_clone`
ranks inserts by how many of the 35 pools they carry the majority of mapped bases in, and one
insert dominates 16 pools at >90% while the runner-up manages 2 at >70%. The ranking is
unchanged from a 0.3 threshold to a 0.9 one. That insert
(`pool33_TTGTCGGT:...:1211-45739`, 44.5 kb, 39 ORFs) contributes 344 MNXR, 295 of them absent
from the GEM lane, of which 172 survive into the host-plus-clone giant component.

**Masking the community solve is not the same measurement as solving the slice.** A row of
the community store is the current a reaction draws in the presence of all 183 inserts, so a
figure restricted to the host and one clone is positioned by current that partly routed
through inserts it does not draw. Measured rather than argued: against a standalone
1,615-reaction EPI300-plus-clone solve, masking the community rows to the same reaction set on
both axes reproduces top-30 neighbours at only **0.58 mean / 0.60 median** overlap. That is
not a truncation artifact — it holds at 0.58 when restricted to rows keeping 30, 60 or 120
partners in both tables — and the mechanism is direct: a median **34%** of such a row's stored
current stays inside the host-plus-clone set at all.

So the community store is reusable for *community-scale* figures, where the drawn network and
the solved network agree, and a named host-plus-clone figure has to be drawn from its own
solve. That solve is cheap (1,615 reactions, 45 s at 12 local workers), which is why this is a
constraint rather than a problem.

### Overlaying a clone on a frozen host layout

A clone figure has to be readable against the host figure, which means the host points may not
move — re-fitting UMAP on the union relocates all 1,489 of them and the two stop being
comparable. `place_clone` instead places the clone's reactions into the existing layout from
their own measured rows, in two stages: initialise at the membership-weighted mean of the k
nearest anchors under UMAP's own `smooth_knn_dist` kernel, then run UMAP's actual optimiser on
the joint fuzzy graph with the host structurally pinned (every edge oriented with a free vertex
as `head`, clone-clone edges supplied both ways, host-host edges omitted, `move_other=False`).
Measured host drift is exactly zero.

The second stage is not decoration. A weighted mean of anchor positions cannot leave their
convex hull and is blind to clone-clone coupling — 252 of 2,139 free-head edges are
clone-to-clone — and the optimiser moves points a median 2.11 off their initialisation against
a nearest-neighbour spacing of 0.149. Leave-one-out over the anchors puts the placement error
at median **0.632, 6% of a random pair**, which is the number to quote for how much a placed
point can be trusted.

Barycentric triangulation was rejected on principle rather than measured: three anchors chosen
in a space spanning fourteen orders of magnitude of current do not bound the target in 2D, so
barycentric coordinates there are extrapolation in a formula that looks like interpolation.

Of the dominant insert's 172 reactions in the giant component, **47 duplicate a reaction the
host genome already encodes** and 125 are new — so "what the insert contributes" and "what the
host lacks" are different sets, and only the `origin` column separates them.

### The circle, and the arc the plane figure could not show

The plane figure was drawn to show the insert *expanding* the host and cannot: an insert
threaded through host metabolism has no island to point at, so expansion reads as scatter.
`ring_layout` fits the same distance to **one periodic coordinate** — UMAP's own optimiser, a
1-D output metric wrapped to the circumference, initialised from the graph's 2D spectral
embedding read as an angle — which frees the radius to carry provenance: host outside, insert
inside. Radius is layer plus an anti-overlap beeswarm and encodes nothing else.

It is a real layout, not a prettier one. The KEGG panel scores mean scatter **0.53–0.58**
across seeds against 0.445 for the plane figure at the defaults and 0.582 at min_dist 1.2. And
it answers the question the plane figure could not: **87 of the insert's 125 new reactions sit
in one contiguous arc** (scatter 0.55–0.64, z = −9 to −13 against 2,000 random 125-subsets),
while the **47 it duplicates from the host are indistinguishable from random** (p 0.23–0.81).
The insert's new chemistry is one localized extension; its redundant chemistry is spread right
round the host. That distinction is invisible in the plane.

`--equalise` respaces the fitted angles uniformly for drawing. It keeps the cyclic order,
which is all a circular embedding carries beyond distances, and closes the gaps UMAP's
clumping leaves — so the host reads as a ring rather than as eight arcs. It also destroys the
gap structure, which is why every statistic above is computed on the fitted angles and never
on the drawn ones.

### The genomic ring, where nothing is fitted

`chromosome_ring` keeps the circle and throws the embedding away: angle is **genomic
position**, and metabolism is carried entirely by the edges. Four rings outward-in — host
genes (DH10B's 4.69 Mb, joined on `old_locus_tag`, which is what the GEM's gene ids are),
host reactions at the circular mean of the genes encoding them, the insert's reactions at
the circular mean of its ORFs, the insert's 39 ORFs over 44.5 kb. Two genome coordinates
sharing one circle, each wrapped once, so an edge crossing between them is the insert's
chemistry reaching into the host at whatever angle its metabolites live.

Nothing is fitted, so two such figures are comparable point for point — which is the property
`place_clone` had to be written to recover for the UMAP figures. What it costs: 99 host
reactions have no gene in the GEM lane and cannot be placed at all, a reaction both genomes
encode is drawn twice because it has two positions, and a multi-subunit complex spread round
the chromosome has a meaningless mean angle (the metrics report the resultant length so that
is visible rather than assumed). The radial spikes are real: one gene encoding a dozen
reactions puts them all at one angle, and on the insert ring those piles are mostly the
unfiltered `clean` lane's EC fan-out — 34 ORFs to 172 reactions — not 172 independent calls.

Edges are circular arcs subtending a constant 60 degrees, bowed so that travel from substrate
to reaction to product is clockwise. A constant subtended angle makes the bow proportional to
the chord, so curvature is a visual family and not a smuggled quantity.

**This coordinate cannot be made sparse by weighting.** The ring figures look sparse because
UMAP puts a metabolite beside its own reactions: their mean edge is 0.152 of the layout's
diameter (median 0.130). Under a genomic coordinate a metabolite's reactions are wherever
their genes happen to be, so the same 4,898 incidences average **0.347** of the diameter --
median 0.723, 5.6x the ring's -- and each one crosses the interior instead of hugging its
arc. The ring figures' own weighting (`--edge-tone bucket`, the default here) therefore draws
*more* visible ink here than it does there, on the same edges. The levers that work are fewer
edges (`--edge-tone log --max-degree`), or none of the host's (`--no-host-edges`), which is
the only setting that makes the insert's reach legible on its own.

Two caveats the origin column exists to let you address later. The four fosmid lanes are
unfiltered, so the `clean` lane's EC fan-out supplies most of the 11,807 insert-only
reactions — the community figure's bulk is as much a CLEAN artifact as a biological claim,
though the named clone's own 344 are not affected. And origin colouring and the KEGG pathway
overlay cannot share the palette; the origin figures drop the pathways.

## Three ways the stored numbers can be wrong

All three are silent — the artifact looks fine either way.

**Warm starting can stop Newton early.** `newton_rhs` declares convergence on energy
stagnation as well as on the reduced gradient, and that third criterion is the safety net for
backflow axes whose gradient never reaches tolerance. From a warm start it can also fire while
the iterate is still short of the minimum: on the 1,489-source GEM sweep, 7 rows (0.5%) differ
from a cold re-solve by more than 1e-3, worst 3.9%, and on exactly those rows the warm solve
took fewer iterations (median 6 against 9). Neighbours barely move — top-30 overlap between
the warm and cold stores is 0.9998 — so this is a numbers-quality defect, not a layout one.
The shipped host store is solved cold. Every store above this line was solved warm.

**A store whose K covers every target is not sorted.** `Store.put` sorts descending only on
the branch that truncates; when K is at least the reaction count it writes the row in natural
reaction order. A cumulative-sum prefix over that is not the strongest partners, it is an
arbitrary half of them — which reads as a plausible density (0.53 instead of 0.07) rather than
as an error. Anything consuming a store's rows in rank order must sort them itself.

**A packed table's `idx` is not in its own row order.** Stored indices are columns of `rxn`,
the target order; rows are `src`. They are the same reactions listed differently, and reading
`idx` as if it indexed `src` pairs every reaction with an unrelated one. The result does not
look like a crash, it looks like a weak result — a placement validated at 78% of a random pair
rather than the 6% it gets once mapped. `knn_from_store` does the mapping; anything else that
opens a store must too.

## Inputs that live outside this directory

* `data/fabfos/processed/metabolism_bake/{vocab,atom_pairs,direction}.parquet` — DVC-tracked,
  built on `capellaz` from MetaNetX 4.5.
* `data/fabfos/originals/metanetx/4.5/reac_xref.tsv` — MNXR -> `kegg.reaction:` cross-reference.
* `KEGG.pathways` — vendored here (92 KB). ModelSEED's copy of the KEGG pathway ->
  reaction table; there is no copy under `data/fabfos/originals/kegg`.
* `data/fabfos/runs/scadc_fosmids/gpr/gpr_4lane.parquet` and
  `data/fabfos/runs/scadc_fosmids/sequences/insert_coverage/insert_coverage_matrix.tsv` — the insert
  reaction calls and the depth-per-(insert, pool) matrix the dominant clone is ranked from.
* `data/fabfos/runs/e_coli_epi300/{gpr,ecspr}/` — where this directory's own durable outputs are
  pinned: the union and named-clone GPR tables, and the EPI300 pairwise I_eff solve plus its
  glucose probe. Everything else it writes is a cache under `cache/`.
* `scratch/mnx_reference/mnx_universe_base_C.pkl` — **gitignored**, and the only input the
  promoted scripts still reach into `scratch/` for. It defines the `medium` reaction set by
  being the graph the retired star figures were built on, which is the sole reason to keep
  it: comparability with those figures.

## Running the sweep off this box

ECSPr is imported from this repo's own `src/`, found `__file__`-relative, so a figure tracks
the package with no install. `ECSPR_SRC` overrides that directory and `ECSPR_BAKE` the bake,
so the scripts run unmodified anywhere the two are staged side by side. Nothing else needs to
travel: numpy, scipy, pandas, a parquet reader, the bake, and a GPR table.

On fir specifically: stage under `/project`, never `/scratch`, which this repo has measured
silently dropping files. The venv wants `--system-site-packages` and `fastparquet` — the
wheelhouse's `pyarrow` will not build. There is no `scikit-sparse`, so every solve falls
through to SuperLU; that is a speed loss and not a correctness one, because `VerifiedSPD`
residual-checks the fallback path and refuses rather than returning it (which is precisely the
library defect noted at the end of this file). A 9,572-source sweep cost 1 GB per worker,
113 GB peak at 120 single-threaded workers, and finished with every row solved and zero
refusals across all 120 blocks. Use one node's worth of forked workers rather than a job array:
array contention is the condition this repo records producing bus errors on fir's overlay
filesystem, and the sweep already splits its source list across forks.

## Open against `src/ecspr/directed.py`: `_SPDReuse`

Three defects, found while running it at 589k unknowns, and inherited verbatim by the
package when ECSPr was extracted (`directed.py:221`, `:226`, `:207/222`). Not fixed there —
this scope wraps it instead (`ieff_ground.VerifiedSPD`), because the wrapper is what the
figure's numbers were validated on.

* The CHOLMOD acceptance test `|Hx - rhs|_inf <= 1e-6 (|rhs|_inf + 1)` is scale-blind: it
  ignores `|H||x|`, so a backward-stable factorization of a kappa~1e9 diode Hessian can be
  discarded over arithmetic no other solver improves on. A relative test is what the
  measurements above ran on, and it never rejected a CHOLMOD result.
* The `_reg_spsolve` fallback's residual is never checked at all — the "safe" path is the
  unverified one.
* `used_cholmod` is a sticky boolean, so the docstring's claim that `splu` is "the sole hot
  path once CHOLMOD punts (50-80% of directed solves)" cannot be checked. Counted properly:
  zero fallbacks in 54 universe solves and 365 medium solves.

Also `figure-net` has `sksparse` and `ecspr:2026.07.14` did not, so anything
promoted into the library must not assume CHOLMOD is present.
