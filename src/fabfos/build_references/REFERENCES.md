# `data/` — every compiled reference and the raw data it requires

The contract `build_references/` implements. **`build_references/` is now a metasmith
transform instance library**, not a directory of scripts:

```
build_references/
  data_types/     raw:: interm:: bench:: buildlib:: lookup::  (build-only namespaces)
  resources/
    buildlib/     ecspr/  -- the whole bake method, VENDORED from src/ecspr by
                              build.sh: the AAM layer stack and its members, the
                              curation sweep, the direction ensemble, the encoding.
                              Generated, gitignored, invoked as `python3 -m ecspr.bake.*`
                  the five flat modules small enough to stay files: the MetaNetX
                  lookups builder and the four benchmark cohort readers.
                  Build side only; never in the wheel.
  transforms/
    acquire/      one per upstream SOURCE FOLDER; nothing here is derived
    bake/         R6 -- the atom-mapping and direction ensembles
    compile/      originals -> the lookups -> the direct refs
    benchmark/    the evaluation set: the host halves, the per-study tier
  build.sh        compiles _metadata/ for this library AND the shipped one
  check_references.py   the checks over a finished run's results
```

New types a **run-side** transform consumes (`ref::mnxr_lookup`) live in
`src/metasmith_libraries/data_types/ref.yml`, not here — `LoadTypeLibraries` keys a
namespace off the YAML filename stem and raises on a duplicate, so a second `ref.yml`
is not possible. `build.sh` passes both type directories.

**All the protocols are implemented.** The graph is split in two, and the halves stay
separate runs because they fail differently and are re-run on different schedules — see
*The runtime split* below for why that used to be a runtime constraint and no longer is.

| half | plan-only gate | executing driver | runtime |
|---|---|---|---|
| annotation (R3, R4, R5, R7) | `examples/annotation_references_dag.py` — 10 steps, **no given at all** | `examples/annotation_references_build.py` | APPTAINER |
| metabolism (R6 and the bake) | `examples/metabolism_references_dag.py` — exactly one given | `tests/build_references_bake_on_hpc.py` | APPTAINER |

The gates plan from nothing, so they resolve on a machine holding none of the bytes; the
executing driver stages the DVC-pinned source folders under `data/fabfos/originals/` instead and
**asserts no acquire transform is in the plan** — a reference built from a fresh pull is
not the reference these pins describe.

The method behind each table lives in `buildlib::` (build side only). The bake half of
it is `ecspr.bake`, a package staged as ONE hashed directory rather than as twenty-one
flat files, because a requirement list that restates an import graph is a list that
drifts from it — and the drift shows up as an ImportError six hours into a queued job.
The five modules that remain flat are the ones nothing else imports. It is all derived
from the previous generation's scripts rather than copied: the AAM recovery sweep's
eleven scripts are consolidated into five proposer lanes plus one arbiter, and the
chemistry tables three of them duplicated verbatim are now one table.
One artifact is deliberately short of its deployed form and says so where it is built:
B3/B4/B5 read the *extracted* cohort tables rather than re-extracting from the papers.
That is not a gap to close — there is no transform that turns a PDF supplement into
rows, and one that only copied a file would dress a judgement call up as a computation.
The extraction is a declared given and the study README says so.

**The metabolism graph has exactly ONE given**, and it is the MetaCyc drop-in — given only
because it is licensed and cannot be fetched. Everything else is produced, including
which three hosts the references are built for: `acquire/host_accessions.py` declares
the host set so that "the reference build" means one thing rather than whatever
accessions a caller happened to pass. A second given appearing in the render is the
signal that something fetchable is being handed in instead.

Each numbered item below is one artifact produced by one transform. **If a folder
under `data/` is not named in this file, it is deleted** — the backup is at
`projects/fabfos/archive/data/` (hardlinked 2026-07-25, 415 files, zero bytes), and
every DVC-pinned chunk is additionally still pinned on `dev`, `dev1` and `dev3`.

## The tier rule

| tier | rule |
|---|---|
| `originals/` | **acquired only** — one folder per upstream *source*, held byte-for-byte as served, under `<source>/<release>/`. Nothing here is produced by a transform in this repo. Two are `given`: licensed or lab-internal, with no URL and no producer. |
| `processed/` | **compiled here**, from `originals/` only. These are the direct refs the run pipeline consumes. |
| `benchmarks/` | compiled here. The evaluation set. |
| ~~`benchmark/`~~ | **retired 2026-08-14.** Held one chunk, the AAM freeze `reference_tier4`. The pin is dropped, not the bytes: the chunk stays reachable from the last commit that carried `data/fabfos/benchmark/reference_tier4.dvc`, which is what versioning data alongside code buys. The atom-pair basis is `processed/metabolism_bake`. |
| `runs/` | **run outputs** — one folder per run (`scadc_fosmids/`, `scadc_metagenome/`, the host strains, and `eydallin_clones/`), each holding the chunks that run produced. A run folder is named `<study>_<material>` where the run belongs to a study, and for the hosts by the strain, which is the whole of what identifies them. Not a reference tier: nothing in `processed/` may depend on it. |
| `nostoc/` | **study data**, not a reference. The three-member community's proteomes and what the run pipeline produced from them. See *`nostoc/`* below. |

**`benchmark/` and `benchmarks/` were one tier spelled two ways** — two branches had
each named the evaluation tier for itself and the merge kept both. Retiring tier4
emptied the singular one, so the collision is gone by subtraction rather than by the
fold that was planned. `benchmarks/` remains pinned at level *one*, the whole tier as a
single 54-file chunk, which is the pin-depth rule below being broken rather than an
exception to it; splitting it per cohort is still open, and is a data job.

**Retiring tier4 leaves consumers pointing at a path that no longer exists.** They fail
loudly on a missing file, which is the good case — but `main/benchmarks/{aska,keio,laser}`
each produced results *on* that basis, so repointing them to the bake invalidates those
results rather than porting them. The two bases are not nested: on carbon the bake adds
4,842 reactions and drops 2,641, and 70 of the reactions iML1515 needs are covered only
by tier4. Anything re-run on the bake is a new measurement.

`curated/` is gone. It held hand-authored judgement with no producer — the ECSPr axis
definitions and the B4 benchmark decisions — and both are recoverable only from the
archive branch. Anything that comes back has to come back as git-tracked text under a
tier that admits it has no transform, not as a fourth tier.

**A pin is two or three levels deep, never four**: `data/<tier>/<chunk>` in the
reference tiers, `data/fabfos/<run>/<chunk>` in the run tier, with `<chunk>.dvc` beside
it either way. That is not cosmetic. `data/.gitignore` re-includes exactly `!/*/*.dvc`
and `!/*/*/*.dvc`, so a pin nested any deeper is invisible to git *and* makes DVC
scatter a second ignore file — and that one `.gitignore` being the only one under
`data/` is the whole invariant the layer rests on. It is also why the hosts are runs
*beside* `fosmids/` rather than nested under a `genomes/` grouping: the grouping would
have pushed every host pin to level four. Check it with
`find data -name .gitignore | wc -l`.
Anything derived that is not a named artifact below is **transient** — it lives in a
metasmith work directory for the length of a build and is not a folder in `data/`.
`data/scratch/` is where those work directories go; nothing in it is pinned and nothing
outside it points in.

**One exception, and it has its own shape: `processed/<tool>/<version>/`.** The raw output
of every tool that produces one is kept, one folder per tool, each holding its version —
`originals/<source>/<release>/` applied one layer down, and for the same reason. A saved
RXNMapper cache says nothing about *which* RXNMapper produced it, and `(metabolite,
canonical rank)` is an **RDKit** canonical rank, so two RDKit versions give one metabolite
two rank systems and two caches that look comparable are not. The version is a directory
name rather than a line in a metadata file because a fact you must open a file to learn is
a fact people stop checking.

Three things were being lost to the work directory, and they are different losses:
LocalMapper's day of CPU (so every downstream method change re-paid it); the per-reaction
refusal reason (so "why did this reaction drop out" was only answerable by re-running the
thing that dropped it); and the *checkability* of the fused table's own provenance — a row
claiming `source=rxnmapper+indigo` is a claim **about** the members, and one that cannot be
checked against what the members said is an assertion with a schema.

Nothing downstream reads these; that is the point. Each folder carries a `manifest.json`
(every tracked package version at write time) and an `INDEX.tsv` (bytes, rows and digest
per file). A file that is missing is recorded as **absent** rather than skipped, because
"produced nothing" and "never ran" are different facts and only one is a bug. Types in
`build_references/data_types/evidence.yml`.

---

## `processed/` — the direct refs

### R1 · host background genome
**Requires:** nothing beyond the host assemblies themselves.
No compile step: the assemblies `acquire/host_genome.py` fetches *are* the
background reference, so R1 is `sequences::isolate_assembly` as a target rather
than a concatenation transform standing between it and the caller.

### R2 · `vector.fna`
**Requires:** nothing. `originals/vector/pcc1.fna` needs no processing at all, so it is not
in the build DAG — a transform that only restates a file is a step that can go wrong
in exchange for nothing. It is a pinned raw file the run pipeline reads directly.

Still worth recording why it is 8 KB: the artifact previously at
`data/fabfos/processed/vector/` was 4.5 MB and held **two** records, pCC1fos plus a 4.7 MB
EPI300 assembly contig. That is a combined vector+background file for the pool-size
estimate, and shipping it as the vector would have made every vector-vs-insert
comparison quietly also a host comparison.

### R3 · `kofam/` → `ko_list` + `profiles/`
Untarred KOfam HMM profiles + the gunzipped KO list.
**Requires:** `originals/kofam/` (the whole source folder).
Both archives are opened here and nowhere else. The acquisition tier keeps `ko_list.gz`
compressed — it used to gunzip it on the way in, which put a file in the originals tier
that no upstream URL would return — so this step must decompress it; the previous `cp`
would have handed kofamscan a gzip stream typed as a TSV. Profiles and ko_list come from
ONE release directory, because pairing a ko_list with profiles from a different build
applies the wrong threshold to every hit, silently.

### R4 · `uniref50.dmnd`
DIAMOND database.
**Requires:** `originals/uniref/` (the whole source folder).

### R5 · `mnxr_lookup.parquet`
The consolidated bridge — `id, id_source, mnxr, evidence_quality` — replacing the three
separate bridge files. Measured 2026-07-25: 30,467,712 rows after dedup, 82.5 MB against
251.5 MB for the three files it replaces; no id collisions across the three id spaces, so
`id_source` is a label rather than a disambiguator. `evidence_quality` costs 0.2 MB and is
kept because `"reviewed"` sorts before `"unreviewed"`, so the existing `keep="first"` dedup
already retains the stronger claim.

**This is every annotation lane's terminus**, not a fourth lane. kofam emits KOs, CLEAN
emits ECs, the DIAMOND lane and the ProteinBERT pool emit UniProt accessions — four id
spaces, and this is the one table that turns all of them into reactions. It is also the
whole of what the build owes the CLEAN lane, which has no artifact of its own (below).

**Requires:** three source folders, not seven files — `originals/{metanetx,kegg,rhea}/`.
Each holds one release directory, resolved at run time rather than named here.
- `metanetx/<rel>/reac_prop.tsv` — EC → MNXR, from the classifs column
- `metanetx/<rel>/reac_xref.tsv` — `kegg.reaction:` → MNXR, and Rhea → MNXR
- `kegg/<rel>/ko2reaction.tsv` — KO → KEGG reaction, from **one call** to KEGG REST's bulk
  `link/reaction/ko` endpoint (~2 s). This retires the dependency on scadc's
  `kegg_requests.db`. The body is saved **verbatim** — headerless, ids namespace-prefixed
  (`ko:K00001<TAB>rn:R00623`) — and the prefixes are stripped here, because an acquisition
  that reshapes puts a file in the originals tier that no upstream URL would return.
  It is deliberately **not** the deployed method, which crawled `get/<ko>` per KO and was
  only ever run over a cached, host-scoped subset: 12,238 rows over 6,220 KOs against the
  deployed 2,738 over 1,356, verified 99.6% identical per KO on the 3,854 KOs where both
  exist. **The kofam lane therefore reaches ~6.5× the reactions it did in the deployed
  build, so its numbers are not comparable to the archived ones.**
- `rhea/<rel>/tsv/rhea2uniprot.tsv` + `rhea2uniprot_trembl.tsv.gz` — UniProt → Rhea

*Not* routed KO → EC → MNXR: measured on these three hosts that takes the kofam lane from
646 to 8,548 reactions and makes 91% of them reactions the EC lane already reaches, which
destroys lane independence.

### L0 · `lookups/{reactions,metabolites,atom_ranks,xrefs,synonyms}.parquet`
Five derived tables every metabolism step reads instead of re-parsing MetaNetX.
**Requires:** `originals/{metanetx,chebi,modelseed}/` + `originals/metacyc/` **[LICENSED]**.

These land under `data/fabfos/processed/` and are **named artifacts**, which is what makes that
legitimate under the tier rule rather than a leak. Naming them was not a convenience:
before them, `chem_prop.tsv` (810 MB) was opened and re-parsed by the pair extractor, both
neural members, the curated member, the direction lane's universe builder and every
curation proposer — six parses with six slightly different ideas of what a trustworthy
formula is, **two** `reac_xref` loaders with opposite collision policies (only one
validated the id shape), and **three** implementations of the canonical atom rank the
entire graph is keyed on. Five near-copies of a parse drift apart while every table still
looks well-formed. That is the failure this retires.

| table | rows | what it settles |
|---|---:|---|
| `reactions` | 83,795 | parsed equation, flags, and **the** reaction SMILES — built once, so every mapper sees the same string and "disagreement between mappers" is not partly disagreement between SMILES builders. 57,593 are buildable. `sub_frags`/`prod_frags` count the `.`-separated fragments each participant contributes: 6,277 metabolites have a multi-fragment SMILES, so 485 reactions produce more templates than participants and were refused as `stripped` for positional bookkeeping rather than chemistry |
| `metabolites` | 1,495,668 | the whole compound table plus per-element C/N/S/P counts (NULL where the formula cannot be trusted) and the InChIKey connectivity block. Whole table, because the curation lanes hunt for structured twins by name and formula and a twin need not appear in any reaction |
| `atom_ranks` | 130,160 | **the node identity contract.** `(mnxm, element) → CanonicalRankAtoms(breakTies=True)` on a sanitised, map-number-stripped molecule, over the 32,540 structured reaction participants. Scoped to the universe because a metabolite in no reaction can never be a graph node; `canonical_smiles` lives here rather than in `metabolites` for the same reason — it is identity, not property |
| `xrefs` | 3,923,902 | both crossreference files as one long table plus `n_mnx_for_source`. Measured on MNXref 4.5: that column is **1 for every row of both files**, so the two loaders that disagreed about whether many-to-one is legal were arguing about a case the data does not contain. Carried anyway, and `xref_map(strict=True)` refuses rather than picks, so a future release that breaks it breaks loudly |
| `synonyms` | 4,007,739 | the structure-supplier index over MetaNetX names + xref descriptions (1.8 M), ChEBI (550 k), ModelSEED (107 k) and MetaCyc (53 k), each row carrying a conservative and an aggressive normalisation. The fuzzy 3-gram index is built in memory from `key_a` rather than stored exploded — same keys, ~40× fewer rows |

### R6 · `metabolism/{atom_pairs,vocab,direction}.parquet`
One artifact in three files, written by **two** steps. Each carries the same
bake-identity block and `refs.assert_same_bake` refuses a mismatched trio — reading
`atom_pairs` against another bake's `vocab` decodes every node to the wrong metabolite
silently. `aam_reference` MINTS that block with the vocabulary and the pairs;
`direction_bake` requires the vocabulary and writes the block through verbatim, so
agreement is structural rather than two computations coinciding. The minting and the
ledger close are one step for that reason: whichever transform mints the block has to be
the one that mints the vocabulary.

The reaction space is the **reaction universe** (`lookup::reactions`), not the union of
the two source tables: a reaction outside the vocabulary falls back to ratio 1.0 in
`ratio_by_code` — fully reversible, more conductance than the evidence supports — and
coding against the universe makes that unreachable rather than contingent. It excludes
MetaNetX's `EMPTY` sentinel, and the encoder refuses any symbol the vocabulary lacks
because the id columns are unsigned and an unknown would land as 4,294,967,295.

**`atom_pairs.parquet`** — the atom-atom mapping, in **six additive layers**, laid down in
order of how much each is worth and each claiming only what the layer below left
unclaimed, then corrected. **Requires:** L0 + `originals/metacyc/` **[LICENSED]**.

| layer | source | what it adds |
|---|---|---|
| curated | MetaCyc `atom-mappings-smiles.dat` — expert-assigned, balances per element at 99.8%+ | **13,620** reactions / 379,217 correspondences |
| whole | the three members' maps of the adjudicated reaction, fused | consensus / single-member / disagreement-diluted |
| completed | the same three over what the **rescue** made mappable — reactions completed with structures proposed for their structure-less participants | the reactions no mapper had ever seen |
| forced | `aam_algebra` — conservation leaves no choice once a participant standing on both sides at equal multiplicity has cancelled | reactions no member is ever given |
| partial_forced | the element reduction's own forced arm | one element of a reaction nothing mapped whole |
| partial_reduced | the three members' maps of those reductions | the same, where a mapper was needed |

**Additive means additive, and the claim is tested.** Four gates at each boundary, all of
which *refuse* rather than warn: the added `(mnxr, element)` is absent from every layer
below; zero collisions on the 6-tuple pair key; no element loses reactions; no negative
ranks. A gate that warns is a gate that gets read once.

**Everything is prepared before any mapper runs, and then each runs ONCE.** The worklist
gives every one of the 83,796 reactions a closed-set verdict and, where it is blocked, the
family of each blocker. Reactions over **600 atoms** are `oversize` and go to no lane —
measured against the deployed table, reactions that large bank at 5.3% and reactions over
1,600 atoms bank at zero, while those are the ones costing minutes each. The rescue then
completes the blocked reactions, and `aam_forecast` names, per `(reaction, element)`, where
a member is expected to return nothing and under which mechanism — three of the six are
exact (a 512-token context window, and our own two caps), the rest are read from the
previous run's records. `aam_partial` builds its element reductions from that forecast
rather than from a finished run, which is the single change that moves it upstream of the
mappers, and `aam_universe` concatenates all three submission classes into the one table
every member reads.

The passes collapsed because **over-offering is free**: the layer stack is additive and its
gates refuse rather than warn, so a reduction built for a reaction that maps fine is never
claimed. Predicting failure wrongly is therefore asymmetric — over-predict and you pay
compute, under-predict and you lose exactly the coverage the reduction would have added —
so the forecast may only ever ADD submissions. It cannot remove one.

Three things that ordering fixes. **LocalMapper is a gap-filler again**: it fills what
Indigo and RXNMapper left, which is the role it actually had in the chain this ports —
487 reactions there, not 57,522. **The rescue's reactions get three votes**: in the deployed
table every rescue-derived reaction is one mapper at half weight, because its crosswalk was
authored after its mappers had run. And **there is one submission string per submission**,
built once and read by all three, so the disagreement the ensemble measures is between
mappers rather than partly between SMILES builders.

**What the rescue refuses, and why it is the largest non-banked outcome.** A curated `*`
body counts as zero atoms for every element, which is only safe when the same body stands
on both sides — so a generic acceptor written on ONE side, with no conjugate partner in the
equation, is refused before the balance test rather than balanced against a molecule that
does not exist. That single rule is most of `rescue_declined`, and it is also why the
conservation-algebra lane banks nothing: its targets are precisely the reactions holding an
unresolved generic. Relaxing it would recover on the order of a thousand reactions and
weaken every balance verdict beneath it; `research/fabfos/benchmarks/aam_r6_verification.md`
measures both sides of that trade.

**The stack is corrected before it becomes a reference.** `aam_redox` refuses every C/N/P
correspondence running between a NAD(P)/FAD/FMN couple and a substrate: a hydride transfer
leaves both carbon skeletons intact, and an MCS mapper cannot see that because hydrogen is
not in the element vocabulary. It is a **repair rather than a filter** — each affected
source atom's surviving arms are rescaled back to the total it started with, so a refusal
concentrates the atom's claim on the destination that survives the invariant instead of
deleting it, and where no arm survives the couple is removed and conservation is asked
about the remainder. Scope is the couple appearing OXIDISED on one side and REDUCED on the
other, per family, so a reaction where NAD is a genuine substrate is untouched; sulfur is
untouched everywhere, because these cofactors carry none and refusing S would be refusing on
a coincidence. The refusals ship beside the corrected table under a named predicate, with
the cofactor resolution that produced them.

**Every reaction ends with an outcome.** `aam_worklist close` joins the adjudication to the
corrected table, so "produced nothing", "never attempted", "offered a reduction and
declined it", "emptied by the repair" and "refused for this reason" are distinguishable
after the build. `tests/build_references_tier4_agreement.py` reads that ledger to break each
miss against the deployed table down by its reason.

**Putting MetaCyc first is a deliberate departure.** The previous generation kept the
neural universe as the base and appended MetaCyc as a small increment, explicitly
declining to let curation *replace* prediction on the 15,539 reactions where both had an
answer. Laying it down first takes that path by construction. It is better founded — on
the shared reactions the two agree 98.6% at molecule level, and where they differ the
curated map is the one that balances — but the result is **not** the deployed table, and
the deployed 63,621 is a floor to clear rather than a number to reproduce.

**Layer 1 went from 1,149 reactions to 13,620 (11.9×), and none of the three fixes was
about chemistry.** Each was measured:
- **`unparseable` 26.9%.** MetaCyc writes `[R:19]` and `[L-cysteine:8]`; RDKit reads
  neither. R-groups are dummied — `*` *is* what an unspecified substituent means, so that
  is a translation. Named residues are **refused**: dummying L-cysteine deletes the sulfur
  that ligates the Fe–S cluster the reaction is about. 2,804 refused, 13,947 then parse
  with zero residual failures.
- **`stripped` 55.5%.** MetaCyc wrote its SMILES over *its own* participant set — it
  writes the water MetaNetX leaves implicit and omits the proton MetaNetX lists — so the
  template and participant counts are expected to differ and carry no information.
  `--align structural` names by structure and caps over-claims by multiplicity.
  `--align strict` is kept for members whose SMILES *we* built, where a count mismatch
  really does mean a molecule went missing and the mapper re-routed its atoms.
- **`no_pairs` 47%.** **Orientation is a convention and the two databases do not share
  it.** MetaNetX writes MNXR100060 as glycolate → glycolaldehyde; MetaCyc writes it the
  other way. Named in the given orientation *every* template misses — 2,451 of 2,500
  sampled. Both orientations are now named and the better one wins, and pairs are always
  emitted in **MetaNetX** orientation, because that is what `direction.parquet` is keyed
  against.

Plus the InChIKey **connectivity** fallback `TIER4_FREEZE` named and recorded as never
attempted: a canonical-SMILES match also tests protonation and tautomer, which the two
databases disagree about while agreeing completely about which molecule they mean.

Checked against the extractor's own worked example: MNXR106432 splits acetyl-CoA's 23
carbons 2 → pyruvate and 21 → CoA, CO₂ contributes its 1, and pyruvate receives nothing
from NADPH.

**`direction.parquet`** — per-reaction `g_rev/g_fwd = exp(ΔG'/RT)`. Three members:
eQuilibrator and dGbyG (both TECRDB-fitted, correlated) and MetaCyc `REACTION-DIRECTION`
(independent).
**Requires:** `originals/metanetx/` (`reac_prop`, `chem_prop`, `chem_xref`, `reac_xref`)
+ `originals/metacyc/reactions.dat` **[LICENSED]** + `originals/equilibrator/`.

The one fitted value in the lane is `canon.DIR_SIGMA_0`, the reversible-default prior
width, and it is fitted on the calibration's own measured arm. So anything that moves
member **coverage** obliges a re-derivation, and every ratio in the table moves with it —
σ₀ sets the shrinkage `λ = σ₀²/(σ₀²+s²)` on every row, including the rows that gained
nothing. `combine` refuses a value outside `DIR_SIGMA_0_BAND` rather than warning, because
a re-derivation that lands outside the band is a finding.
dGbyG weights are **vendored code**, not data — they belong in the library, not `data/`.
They are the 100 `.pt` heads of `models/mpnn_A139_B23_E300_L2_v2/`, ~105 MB, and they are
**baked into the `:dgbyg` image by a git clone, not by pip**: they sit at the repository
root outside `src/dGbyG/`, no packaging declares them, and `api.py` locates them with
`__file__.split('src')[0]`, which resolves to nonsense from site-packages. A
`pip install git+…` therefore yields an importable member that cannot load a single head —
which is how the member sat "installed" and unused for a generation.

**The identity block cannot tell two direction tables apart, and that is deliberate.** It
is a fact about the *node space*, so a direction-only re-bake inherits its predecessor's
identity byte for byte — r8 carries r7's `0ffd4c8c6231696e…`, and `assert_same_bake` is
right to hold across the mixed trio while it is being built. What separates them is
`direction.parquet`'s per-file `src_direction_sha256`, under a **different** footer key
(`ecspr_bake_file`) precisely so the trio check does not compare it across three files that
legitimately differ. Quote that field, never the identity, when asked which direction table
something holds — and anything memoising a decode of this table must key on **both**, or it
will serve a stale extract to a caller that checked the identity and was told the truth.

**The chunk's `seams/direction_annotation.parquet` is that table's provenance record**: its
sha256 is what `src_direction_sha256` names. Nothing compares the two, so a chunk assembled
by hand can be left pointing at an annotation it does not carry, silently — swap the seam
whenever the table is swapped.

**`vocab.parquet`** — int-code vocabulary. Derived from the two above, no additional raw.

**The release directory is resolved at run time, not named here**, exactly as R5 does it:
each requirement is a whole source folder holding one release, and the transform refuses
if it holds two. Which MNXref build the equations, the structures and the two crosswalks
come from has to be one answer — an equation from one release aligned against a compound
set from another is not a case to resolve by taking the newest.

**The MetaCyc carve happens in these two transforms, not at acquisition.** `metacyc/` is a
staged given with no transform at all: it is licensed, so nothing may fetch it, and a
transform that only verifies a directory it did not create does no work and moves the
failure one step later. Both drop-in shapes are accepted — `<release>/data/*.dat` as
unpacked, or the `.dat` files at the top of `<release>/` if flattened by hand — and the
refusal names which ensemble just lost its independent member.

**eQuilibrator's cache is presented through `XDG_CACHE_HOME`, which is the only lever
there is.** `equilibrator_cache.zenodo.get_cached_filepath` resolves its directory with
`pooch.os_cache("equilibrator")` — `$XDG_CACHE_HOME/equilibrator/<file>` — and reads no
other variable. `EQUILIBRATOR_CACHE_DIR`, which both the acquisition and this step used to
export, is read by *nothing*; what actually made the old build work was that the
acquisition left the package's own nested layout in place, so the same root happened to
line up. Now that the two artifacts are lifted into `equilibrator/<version>/`, the
consumer rebuilds the `equilibrator/` level by symlink (compounds.sqlite is 1.3 GB and
this runs once per member). Pooch re-checks its embedded md5 before using either file, so
a wrong or truncated staged cache is caught at the consumer too.

### R7 · `label_transfer_landmarks/` — the labelled landmarks  ⚠ CHANGED, see *Open decisions*
Swiss-Prot sequences, embedded with ProteinBERT, labelled with MetaNetX reaction ids
mapped through Rhea. Those three clauses are one sentence and each is load-bearing.
**Requires:** `originals/swissprot/` + R5.

**The sequence source is Swiss-Prot, not UniRef50, because the cut and the sequences
then describe the same set.** The pool is defined by the bridge's `reviewed` rows, and
`reviewed` means the accession came from `rhea2uniprot.tsv` rather than
`rhea2uniprot_trembl.tsv.gz` — i.e. it means Swiss-Prot, exactly. Taking the sequences
from UniRef50 instead meant an accession only had a sequence if it happened to be its
cluster's *representative*; UniRef50 clusters at 50% identity, so entries sitting under
another entry's representative dropped out silently. That was counted rather than
substituted for, but it was a coverage loss with no upside once ~93 MB of exactly the
right sequences is already a source folder in the graph — and it drops an 8.8 GB gzip
stream out of this transform's inputs. The transform refuses if more than 2% of the
reviewed accessions have no Swiss-Prot entry, since that would mean Rhea and UniProt
have drifted far enough apart that the pool is quietly a subset of what it claims.

The `reviewed` slice is 365,240 rows against 35,397,466 unreviewed, landing in the same
order as the deployed pool's 273,764 — but it is not that pool; see *Open decisions*.

**One parquet, one row per accession** — the accession, its MNXR labels and its 512
floats side by side. It was an index parquet beside an `.npy` stack addressed by row,
and it shipped scrambled: `pbert` writes fixed 1,024-sequence chunks named `<stem>.1`,
`<stem>.2`, … with no zero padding, the assemble step stacked `sorted(glob("*.npy"))`,
and lexicographic order puts chunk 10 before chunk 2 while the index stays in FASTA
order. Every one of the 222,019 references then carried another protein's reactions,
the length check passed and the label merge passed. The chunks are now stacked by their
integer suffix and the id order is checked against the FASTA the transform itself wrote
before a label is attached. **Every `pbert` column produced before this fix measured
the misalignment rather than the lane.**

### R10 · `label_transfer_landmarks_esmc/` — the same landmarks, embedded with ESM-C
The same bridge cut, the same Swiss-Prot release, the same accessions as R7, embedded
with ESM-C 600M so `gpr_7lane`'s ESM-C kNN lane has a pool of its own to vote against.
**Requires:** `originals/swissprot/` + R5 + R8.

**Two references rather than one directory with two stacks.** The embedders live in two
images — `proteinbert.env` carries no ESM-C SDK and `esmc.env` carries no ProteinBERT —
and only the ESM-C pass needs a GPU, so folding them together would make every
ProteinBERT rebuild queue for a device and put both stacks' fate in one exit code. Each
carries its accessions in the rows of its own embeddings, so neither can be paired with
the other's vectors by accident.

**A pool is only comparable to a query if the same function produced both**, so the
transform's inference block is `functionalAnnotation/esm_c.py`'s, copied — same weights,
same `max_len`, same sliding-window aggregation with the same length weights, same mean
over non-special tokens. The two must be changed together, and both files say so. What
is dropped is the per-layer means: EZpred's heads are not in this path, and the stack
would be 3 GB of nothing.

Unlike R7 it does **not** recode rare residues. That transform recodes because
ProteinBERT's encoder indexes past the end of its lookup array on ordinal 90 (`Z`) and
dies after the model has loaded; ESM-C maps what it does not know to its unknown token,
and the query lane does not recode either — so recoding here would make the pool and the
query disagree about what a rare residue is.

### CLEAN — no artifact, deliberately
The fourth annotation lane has **no compiled reference and no acquisition**. ESM-1b and
the max-separation bundle are baked into `docker://quay.io/hallamlab/external_clean:2026.06.14`
at `/app`, so under a container runtime the weights are already present and there is
nothing for a transform to produce. The same holds for ProteinBERT's weights: R7 builds
the labelled *landmarks*, never the model. The build's entire obligation to the CLEAN lane is
R5's `ec` route.

**The asymmetry is the point, and it is not arbitrary.** Two of the seven lanes get a
weights artifact and two do not, because two ship their weights in their image and two
do not:

| lane | weights | artifact |
|---|---|---|
| CLEAN | baked into `external_clean` | none, and never will be |
| ProteinBERT | baked into `external_proteinbert` | none — R7 is the labelled *landmark set* |
| ESM-C | the SDK fetches it on first use | **R8** (+ **R10**, the labelled pool) |
| EZpred | Zenodo drops, plus a source tree | **R9** (no producer — see below) |

A first-use download inside a step is the thing a reference exists to replace: it makes
every run depend on an endpoint being up and on an account still having accepted terms,
and it records nothing about which revision ran.

### R8 · `esmc_600m.tgz` — the ESM-C 600M weights
Shared by the ESM-C embedding lane and the EZpred EC heads, which is why the 600M pass
runs once and both consume it.
**Requires:** `originals/esm_c/` (the whole source folder).

**The gate is gone, and the repository id changed with it.**
`EvolutionaryScale/esmc-600m-2024-12` now 307-redirects to `biohub/esmc-600m-2024-12`,
which answers `"private": false` and serves the checkpoint to a machine holding no
token; the blob hashes identically under both names
(`8ef856e1…`, 2,300,275,866 B). The acquisition fetches the redirect target directly, so
the revision it records is the one actually served rather than one hop upstream of it,
and it uses `HF_TOKEN` when present without ever requiring it. It still checks the
member's size, because a truncated transfer and an error body both leave a file that
exists.

The revision is resolved from the server before any bytes move and names the release
directory, because the default branch rolls. The compile verifies
`data/weights/esmc_600m_2024_12_v0.pth` by name and size **from the tar index**, without
unpacking 2 GB: the ESM SDK's `from_pretrained` takes registered model *names* only and
resolves that exact relative path from the process's cwd, so a moved checkpoint is a
silent failure and a truncated one is a failure after the GPU hours.

**There is a duplicate producer**, and it is why every plan here excludes `logistics/`:
`transforms/logistics/downloadEsmC.py` in the shipped library produces the same type by
fetching it at run time. Two producers for one reference makes provenance a planner
tiebreak, so each gate asserts that downloader is absent by name.

### R9 · the EZpred DL-only bundle — **staged, not produced**
`acquire/ezpred.py` is done: the two immutable Zenodo records (`models.zip`, the enzyme
/ non-enzyme MLP ensembles; `Data2.zip`, the label IA tables) land byte-for-byte as
served, and both ids name the release because a mismatched pair silently relabels every
prediction — the heads' output columns are indexed by the IA tables' label order.

`compile/ezpred_model.py` is written and **parked in `transforms/_deferred/`**, which
`build.sh` skips along with any underscore-prefixed directory. It needs the EZpred
*source tree*, and where patched source lives is unresolved: our copy carries the
DL-only fork (no MMseqs2 homolog augmentation, no Foldseek template fusion), so it is
not what any URL returns and cannot sit in `originals/`. Its shape objection to
`buildlib::` no longer holds — `buildlib::ecspr` is a directory-typed entry with a tree
digest for an identity, which is exactly the resolution
`transforms/_deferred/README.md` names as the second candidate. Where the patched source
should LIVE is still open.

**The lane runs regardless.** The assembled bundle is staged at
`<processed>/ezpred_model/EZpred` and declared as a given in `examples/scadc_gpr.py`'s
`REFS_7`, so what the open decision gates is *rebuilding* the bundle on a clean machine,
not using it. The lane's numbers are real; its reproducibility rests on a pin plus a
README rather than on the graph.

### The runtime split
`Agent.runtime` is one global setting, so a graph whose envs cannot all satisfy it does
not run. Both halves now run under **APPTAINER**; they stay separate runs because they
fail differently and are re-run on different schedules, not because a runtime forces it.

R6 used to be MAMBA-only, and the reason was simply that `rdkit.env`, `equilibrator.env`
and `cobra.env` carried a `conda:` key and no `container:`. `docker/ecspr_bake` supplies
the first two as **three** images — `:aam` (the mapping stack + the curation arbiter),
`:direction` (the eQuilibrator member + the combiner) and `:dgbyg` (the learned member).
The splits have two different causes, and conflating them is what kept dGbyG deferred
past its reason. `:aam` cannot join the others on **numpy**: `equilibrator-cache` 0.7.1
requires `numpy>=2`, and `torch` 2.2.1 — which both neural mappers need — is compiled
against the numpy 1.x C API. Merged, torch reports
`Failed to initialize NumPy: _ARRAY_API not found` as a *warning* and runs on with its
tensor↔numpy bridge broken, so each image asserts its own pins at build time. `:dgbyg` is
separate on **python**: dGbyG's source uses a same-quote nested f-string and needs 3.12 to
parse, while the eQuilibrator stack is pinned at 3.11. Under 3.11 it installs and then
raises `SyntaxError` at import — not `ImportError`, so the unavailable-member guard in
`dir_drive` never caught it and the step died rather than degrading to a missing vote. `cobra.env` is untouched — it is
read by `benchmark/host_gpr_gem.py`, outside R6.

The SIF is **synced, not pulled**: `dev.sh --build --sif --sync` builds from the local
docker daemon and rsyncs into the cluster's apptainer image store under the name
`Environment._cached_name()` derives from the URI, so no registry is ever contacted.
`tests/build_references_bake_on_hpc.py` plans R6 here and runs it there; it is the only
driver, since the method is the graph.

| gate | steps | given |
|---|---|---|
| `examples/annotation_references_dag.py` | 10 — 6 acquisitions, 4 compiles | **none at all** |
| `examples/metabolism_references_dag.py` | 12 — 4 acquisitions, the lookups, 5 tool lanes, 2 assemblies | **exactly one** — MetaCyc |

The annotation half is reproducible from the library alone on a machine holding none of
the bytes: every leaf is a download with a transform behind it. The metabolism half has
one leaf that is not, and its gate asserts the count rather than the name — a *second*
given appearing there means something fetchable is being handed in instead of produced.
Both gates plan without the bytes, including the licensed drop-in: planning is type-driven
and never opens an input, so an empty stand-in resolves `fabfos_data::metacyc` exactly as
23,557 real files do. A run is what needs the real one, and it refuses where the `.dat` is
read.

---

## `benchmark/`

**`runs/census.tsv` answers "what does this run have".** Four columns — `run`, `denovo`,
`gem`, `gem_adapted_from` — regenerated by `runs_census.py`, never typed. It measures
rather than declares: `denovo` is the ORF overlap between a run's lanes and its own
de-novo table, following a borrow to the parent whose lanes back a derived run.

Networks are **not stored**. Each x in X is constructed at run time from the conditions
table plus two GPR tables — the host's, which is the background network, and the study's,
which carries the edges each condition adds or deletes. Both are the GPR schema's core
plus the `attribution`, `feature` and `universe` blocks, so the two concatenate without
reshaping; the study table adds the `cohort` block (`condition_id`, `cohort`, `action`,
`source_organism`) that scopes a row to a condition. `lib::fabfos_evidence` declares all
of it, and every producer on both sides validates against it before writing.

**The cut is by PUBLICATION, not by processing stage.** `benchmarks/<study>/` holds one
study each, on one file schema, so a study can be added, revised or withdrawn without
touching any other — and each is scored on its own terms. The seven are `laser`, `keio`,
`eydallin` and the four contrast cases (`aromatic`, `pg_anionic`, `forsberg`,
`fa_supply`) that were curated and never wired in.

Every study folder holds exactly `extraction.tsv`, `gpr_manual.parquet`,
`conditions.tsv`, `Y/` and `README.md`. A folder missing one is a named failure; a folder
with an extra one is a schema violation, and `study_tier` checks both.

### B1 · `runs/<host>/gpr/gpr_gem.parquet`  (one per host with a model or a borrow)
The GPR a curated genome-scale model asserts. One job over the host set — the tier
delivers it as one folder, and per-host jobs would need the planner to split that into
per-host givens, where a requirement binds ONE concrete type and sibling subtypes silently
collapse. The transform's own layout inside that folder is `hosts/<host>/`, which is why
`check_{ag1,epi300,lw06}_identity.py` build that path under their `--gpr` argument; the
driver fans it out per host on publish, so a run's GEM sits beside everything else about
that run. `BUILD_gem.json` describes the build rather than any one host and sits once, at
`runs/`.
**Requires:** `originals/genomes/` (the curated GEMs), `originals/metanetx/` for the
BiGG → MNXR crosswalk, R5, and the metabolism bake for `in_atom_universe`.

The host set is `acquire/genomes.py`'s and the borrow map is `host_gpr_gem.py`'s; neither
is listed here, because a roster in a doc is the thing that rots. **Two hosts have no
model of their own**: EPI300 borrows DH10B's, AG1 borrows DH1's. W3110 is in the set with
no GEM at all — it is where an ASKA clone's SEQUENCE comes from, not a strain a model is
read against.

**EPI300's borrow of DH10B's model is not free.** `proV` and `fhuA` are pseudogenes in
EPI300 — both in AND clauses, so seven transport reactions go dark, and nothing else in
the model's 1,327 genes differs. Earlier work here recorded the edit list as empty, which
was reachable two ways and wrong both times: a pseudogene is a CDS with no
`/protein_id`, so a check keyed on the protein accession never sees one, and the two
strains share no id space, so a locus-tag join instead reads 1,272 of 1,304 genes as
deleted. The join that means something is the protein — exact sequence where it matches,
symbol otherwise. `check_epi300_identity.py` asserts the edit list IS that pair costing
those seven reactions, rather than asserting it is empty.

**AG1's borrow of DH1's is not free either, and there is no genome to compare.** AG1 is
absent from NCBI, so the licence is the genotype Qimron et al. state — seven markers —
and `check_ag1_identity.py` measures each against the model rather than reasoning about
what they sound like. Five name no gene in it. `relA1` names `relA`, which sits alone on
`GTPDPK` and beside `spoT` on `GDPDPK`, so the edit is one reaction and the isozyme keeps
the other. Unlike EPI300's seven, it is INSIDE the atom universe: this borrow moves the
network. `thi-1` is a classical allele rather than a locus and is deliberately NOT edited
— it is a claim about the medium, and picking one of the thiamine module's reactions
would put a fabricated deletion in every eydallin condition's background.

### B2 · `runs/<host>/gpr/gpr_denovo.parquet`  (one per host proteome)
The GPR that host's own annotation lanes infer, produced by the **shipped** 4-lane mapper
running on each proteome exactly as it runs on a fosmid ORF set. That is the point of
wiring it this way: it makes the benchmark a test of the method rather than of a file
someone once produced.
**Requires:** `originals/genomes/` → `host_proteomes` → the annotation lanes → R5.
`examples/benchmark_hosts_denovo_on_hpc.py --site {fir,sockeye}`, 7 steps, GPU for CLEAN.

**IT RUNS ON EITHER CLUSTER, AND THAT IS WHY THE DRIVER TAKES A SITE.** fir drops into
whole-cluster cooling maintenance for a day and a half at a time; its login nodes and
storage stay up through the window while the scheduler refuses every job, so the work
moves rather than waits. `--site` switches host, agent home, module incantation, image
store, both SLURM accounts and the GPU declaration *together* — they are not
independently choosable. Naming fir's H100 in a sockeye submission is the trap: sockeye's
`job_submit` plugin rejects a typed `--gpus-per-node=v100:N` by resolving it to
`requested_gpus 0`, so CLEAN runs on no card instead of failing to submit.

The plan SHAPE does not depend on the site — same seven steps, same transforms, same
targets — but the task KEY does (`nN9rh3Yd` on fir, `UhuDTS6t` on sockeye), because an
input's declared path is part of it and the two clusters keep the references in different
places. That is the right behaviour: a run against a different copy of the references is
a different run, and nothing should share a cache across it.

**Moving the references between clusters is a globus job, not an rsync.** The 4-lane set
is ~25 GB and the five lane images another ~12 GB; both clusters have endpoints
(`alliancecan#fir-globus`, `ubcarc#sockeye`), so the transfer is server-to-server and
never touches the workstation or the UBC VPN. The two identities that authorise it are
already in one globus session.

`host_proteomes` is the scatter and it is in the graph rather than in the driver for a
lineage reason: B2 pins the mapper's output to `parents={genomes}`, and three proteomes
staged as unrelated givens give that pin nothing to bind to.

**A B2 table and the `runs/<host>/annotations/lanes/` it came from share one ORF
namespace** — `lcl|<accession>_prot_<id>_<n>`, the headers of the `.faa` under
`originals/genomes/<host>/genome/`. That is what makes a cross-host comparison at ORF
grain mean anything, and `runs/census.tsv` measures it per run rather than asserting it:
the `denovo` column is an overlap count, not a directory test.

It reads that way because it did not use to. Until the 2026-08-23 campaign, six hosts
had a de-novo table with no lanes behind it in the tree at all, and k12, dh10b and
epi300 carried a *2024* lane run keyed on an earlier ORF call (`NP_414542.1`,
`ECDH10B_0001`, `C1_1`) that overlapped their own tables by **0 ORFs**. Joining to it
returned nothing, silently. Those trees are gone, which also leaves
`derive_annotation_faa.py` without a subject — it existed to keep the 2024 tree
self-consistent, and pointing it at the current lanes would write a `.faa` that only
duplicates the registry proteome.
**The lane set is the table's contract, and it is checked.** A B2 table carries exactly
the four channels `lib::fabfos_evidence.LANE_SETS["chosen_4"]` declares; the collector
refuses by name when the mapper's output does not, and its `BUILD.json` records the set it
checked. The fourth lane's reference is R7 above, built by `compile/label_transfer_landmarks.py`
— an absent pool is a staging failure that stops the run, never a shorter table.

### B3 · `<study>/gpr_manual.parquet`  (7 studies)
Each study's edges, as the curator read them. Where the extraction attributed reactions
per gene, so does the table; where it attributed them to the OBSERVATION — LASER's
`genes_json` names each gene's action but no gene carries its own MNXR — the rows are
`feature_kind=curated_set` and `orf` names the whole gene SET rather than a gene.
Splitting the list across an observation's genes would manufacture an attribution the
curator never made.
**Requires:** `<study>/extraction.tsv` (a given), R5, the bake, and B1 for the background.

**The four contrast extractions are RAGGED and pandas does not say so.** Their `add` rows
carry no src/sink metabolites and were written with three tabs where the header wants
four columns, so every field right of `sink_name` shifts left: `element` reads the
direction and `expected_dir` reads the citation. The repair pads the src/sink block and
then validates that both columns carry legal values, refusing rather than guessing.

### B4 · `<study>/conditions.tsv`
One row per (condition, element). Names its host, its element, the edges it adds and
deletes, and the control it is read against. **A condition may only be emitted if that
study's GPR table knows the edge set it names**, so the conditions are built FROM the
table rather than beside it.

**Which elements a study is scored on is a property of its readout, not of the tier.**
The default is all four, which is right when the observable is growth or fitness; a study
whose observable is a named compound gets `elements` in its `STUDIES` entry and is emitted
on that element alone. `eydallin` measures glycogen, so it is C-only — the N/P/S copies of
its rows asserted three directions the paper never measured, and nothing downstream could
tell them from the one it did.

Four control kinds, declared per study rather than inferred: `baseline` (the unperturbed
host — the zero point every result is a difference from), `structural` (a perturbation
that cannot reach the network, so it must return zero), `on_path` (an in-base atom-mapped
edge, so it must move) and `declared` (the curator's own, which is the whole reason the
four contrast cohorts were chosen).

A structural control here is a condition whose reactions have **no atom-pair coverage** —
a property of the reactions. The prior implementation tested endpoint PAIRS, so a reaction
adding only parallel edges read as unreachable and 27 real effects were relabelled
controls, manufacturing a noise floor out of signal.

### B5 · `<study>/Y/expectations.tsv`
The answer key, keyed on `(condition_id, element, mnxm)` with `expected_dir`, `basis` and
`tier`. It moved from edges to metabolites because the measurement did: the probe used to
report a conductance between two chosen terminals, and universal leakage reports an
effective current for every metabolite.

Sparse under one declared default: any triple absent from the table is `0`.

Three rules, each learned the hard way. **Direction comes from what was measured**, not
from what someone predicted — `expected` is populated on 10 of 382 GOF rows, so using it
would shrink the key to the cases already anticipated. **A condition whose direction is
unknown contributes no rows at all**, not a row of zeros: "we don't know" and "we expect
no movement" are different claims. **A metabolite two of a condition's reactions disagree
about gets no expectation**, rather than a coin-flip.

**`eydallin/Y/measured_glycogen.tsv` is the one file in a study folder this tier does not
write.** Eydallin's screen publishes no data table — its 86 values exist only as the bars
of Fig. 1 — so `main/benchmarks/eydallin/digitize_fig1.py` measures them off the JPEG and
drops the result here. A `study_tier` run emits a fresh `Y/` without it; re-run that script
after one, or the cohort silently loses its magnitudes and keeps only their signs.

**Nothing on Y's input path may be an ECSPr result.** A key derived from the
implementation cannot fail. The line is precise and the loose version is wrong: Y may read
the network's STRUCTURE — which metabolites a reaction's atoms flow into is a fact about
the model — but never a RESULT. `audit_y_inputs` enforces it over the paths the step opens.

---

## `curated/` — hand-authored, no producer

### C1 · `ECSPr_axes/biomass_dag_axes_set4.json`
The 79 biomass source→sink axis **definitions**. Graph-independent: it is set4 itself, so
it is the same table regardless of which universe the solve runs on. Declared
`producer: hand-curated`, `determinism: frozen` in the incumbent provenance. 28 KB — git
text, not a DVC chunk. The pin (`data/fabfos/processed/ECSPr_axes.dvc`) had no content;
the file has to be recovered from the incumbent tree.

### C2 · `benchmark_decisions/`
`gene_reaction_overrides.tsv`, `host_lineage.tsv`, `target_resolution.tsv` — curated
judgement calls feeding B4. Currently DVC bulk inside `benchmark/v3/decisions/`; small
enough to be git text.

---

## `originals/` — the acquisition set

Every source named by an artifact above, and nothing else. One folder per SOURCE, each
holding one `<release>/` — see `build_references/data_types/fabfos_data.yml` for what
each release name is a claim about.

| source | upstream | note |
|---|---|---|
| `metanetx/` | MetaNetX | 4 tsv + .md5 sidecars; feeds R5, R6, B1 |
| `rhea/` | Rhea FTP | R5 |
| `kofam/` | `ftp.genome.jp/pub/db/kofam` | R3; tarball and list kept as served |
| `uniref/` | UniProt (EBI mirror) | R4 |
| `swissprot/` | UniProt (EBI mirror) | R7's sequences |
| `kegg/` | KEGG REST | R5; bodies kept verbatim, parsed at compile |
| `metacyc/` | **LICENSED — manual drop-in** | R6 both halves. **A GIVEN: no transform, ever.** Carved where it is read |
| `equilibrator/` | eQuilibrator compound cache | R6 direction |
| `chebi/` | EBI ChEBI, `archive/rel<N>/flat_files/` | R6 curation supplier lane. The **archive**, not the rolling `flat_files/` path — frozen at its own URL, so the release is a real pin |
| `modelseed/` | ModelSEED biochemistry | R6 curation supplier lane. Pinned by the **commit** that last touched the file; the repo publishes no releases |
| `genomes/` | NCBI assemblies + BiGG models | R1, B1, B2 |
| `vector/` | pCC1 | R2. **A GIVEN:** lab-internal, unpublished, no upstream URL |
| `literature/` | `laser/`, `keio/`, `eydallin/` — three cohorts — plus `het_screen/`, which is **not** a fourth: it is LASER's heterologous slice resolved to UniProt, joined on `(label, source organism)` rather than concatenated. `curate_het_screen.py` produces it | B3, B4 |
---

## `nostoc/` — study data for the three-member community

Not a reference tier. Nothing in `processed/` or `benchmarks/` reads it, and no artifact
above depends on it; it is here because the run pipeline's *inputs and outputs* for the
`Ana_PS` community (BioProject PRJNA1405787) need the same two-level pin and the same
"named here or deleted" rule as everything else under `data/`.

| chunk | what |
|---|---|
| `orfs/` | `{NOS,ERY,RHI}.faa` — prodigal proteomes over the three hifiasm-meta MAGs. **A GIVEN:** produced outside this repo, no transform. 5,930 / 3,192 / 4,402 records. The stem is the organism key every downstream table joins on |
| `annotation/` | `<organism>/{gpr_4lane.parquet, <organism>.faa}` plus `PROVENANCE.md` — the canonical four-lane GPR tables and the proteome each describes. Produced by `research/fabfos/examples/nostoc_gpr.py` on fir, over `src/fabfos/pipelines/annotation.py`, from `orfs/` and five `processed/` refs |
| `ecspr/` | the composed community measurement: `networks/` (seven composed graphs, their bridge reports and condition sets), `results/` (the 21 measured units), `figures/`, and the composition's own reference frames. Produced by `research/fabfos/examples/nostoc_ecspr.py` |

The proteomes are pinned rather than symlinked into the workspace data share because the
organism key is the file *stem*: a run whose only record of which bytes were `NOS` is a
symlink target is not reproducible.

`ecspr/results/` IS THE MEASUREMENT OF RECORD AND CANNOT BE REPRODUCED BY THE CURRENT
LIBRARY. It was measured by two transforms, one per probe, writing a wide table each. The
library has since collapsed both probes into one `ecspr::results` type — a long table
keyed by a `probe` column — produced by `ecspr_measure` dispatching the `ecspr`
command-line tool, and it declares no two-point step at all. The chunk is what any claim
about this community rests on, and `research/fabfos/examples/nostoc_ecspr_verify.py
--products` is what re-checks it.

**`ecspr/networks/*/conditions_*.parquet` are readable history, not inputs.** They are the
pre-split shape: one row per (condition, *sink*), with a `mode` column the two transforms
filtered on themselves — 92 rows for the NOS singleton. `ecspr.model.conditions.read` has no
`mode` and reads every row as its own condition, so those 92 become 92 one-sink ground
solves where `ecspr.model.compose.make_conditions` now intends four, one per element, each
naming every precursor at once. Nothing raises; the numbers are just a different
measurement. `nostoc_ecspr.py --compose` writes the current shape beside them and
`check_conditions` refuses the old one, which is the only thing standing between a re-run
and a plausible wrong answer.

**So are `ecspr/networks/*/{atom_pairs,gpr,direction}.parquet`, and this one has no
refusal in front of it.** They were composed from a bake older than the deployed one and
record nowhere which bake that was, so `nostoc_ecspr_verify.py --structural` — which
compares a freshly built graph against them — fails by construction and will keep failing
until they are recomposed. Measured rather than inferred: the live graph is 167,375 nodes
against the artifact's 162,801, and node count does not depend on the direction table, so
the divergence predates any direction re-bake. Recompose before reading `--structural` as
a verdict on anything.

---

## Deletions

Everything under `data/` not named above.

| deleted | why |
|---|---|
| `raw/AAM/{chebi,bigg,modelseed}` | Declared as external references in the incumbent provenance, but **no code in the tree loads any of their filenames** — verified by grep across `methods/`, `src/` and `transforms/`. `atom_pairs`' declared inputs are `metanetx.reac_prop`, `metanetx.chem_prop`, `metacyc26.flatfiles` — these three are not among them. Carried over wholesale from scadc's `references/`. |
| `raw/AAM/` as a grouping | AAM is a *method*, not a source. It also hid the licensing boundary: `metacyc26_flatfiles` is licensed and the other three are not. `metacyc/` becomes its own chunk. |
| `raw/direction/*.parquet` | Six intermediates of the R6 direction chain — `calibration`, `curated_per_mnxr`, `direction_annotation`, `member_dgbyg_base`, `member_eq_base`, `netA_gem_direction`. Transient by the tier rule. |
| `raw/mnxref-4_5/` | Derived (it *is* a pre-baked R6), and it additionally carries ECSPr **run outputs** — `graph/`, `solve/`, `solve_directed/`, `null/`, `null_directed/`, `ground_probe/`, `worklist*.tsv` — which belong in neither tier. Deleting it means R6 can only be rebuilt through the full ported chain, which is the point. |
| `data/_old/` | Superseded by the archive backup. Exception: `external/pathways_2026/reference_proteins.{faa,tsv}` survives *only* under R7 option A. |
| `reference/functional_annotation/bridges/` | Superseded by R5. |
| `benchmark/v3/{base_graphs,universe}` | Networks are built at run time from B1+B3+B4. |
| `benchmark/v3/{baseline,observations}` | Run outputs, not benchmark definition. |
| `benchmark/v3/{contract,provenance,v1}` | Superseded by B4/B5 and by the library's own index. |
| `reference/_metadata/`, `benchmark/_metadata/` | Byte-identical copies (`cb2abe78…`) of the **old** `.awm/data/ref` library index — 911 items over `external/ derived/ validation/` tiers that describe a layout this tree no longer has. Dropped into two places by the clean-slate commit. |
| `reference/functional_annotation/{kofam,rhea,uniref50}.dvc` | Stale pins naming directories that no longer exist; their content moved to `raw/`. |
| `reference/hosts/` | Emptied already; B1/B2 now land in `fabfos/<host>/gpr/`. |
| `fabfos/e_coli_epi300/annotation_alts/raw/` | The 2024 MetaPathways + InterProScan run directories, 101 MB. Its `blast_results` are already distilled into the `cazy`/`metacyc`/`swissprot`/`uniref90` alt tables, and its one irreplaceable file — `orf_prediction/epi300.faa`, the ORF set the epi300 lanes were actually built on — was lifted into `annotations/epi300.faa` first, remapped from the run's `epi300-C<n>-G1` headers into the lanes' `C1_<n>` space. The InterProScan output was never distilled into an alt table and goes with it. All 35 files stay recoverable from the `genomes.dvc` pin at commit `9f18c1f` until someone runs `dvc gc`. |
---

## Open decisions

**R7 — the labelled landmarks. RESOLVED in the contract, open in its consequence.**
The pool is now built from Swiss-Prot (2026_02, 222,019 reviewed sequences) labelled
through the `mnxr_lookup` bridge rather than from a separate labelled proteome, which
removes an acquisition and the KEGG licensing question with it. The built artifact
records its own provenance in `pool_source.txt`. What
stays open is that this is *not* the deployed pool: that one is KEGG-derived (54,005
sequences keyed on KEGG gene ids, labelled by KO), so the `pbert_transfer` lane's
numbers will move and must not be reported as a reproduction of the deployed lane.
Porting `benchmark_v4`, which carries its own pool calibration and verification steps,
is where that gets settled.

*The superseded framing, kept because the measurement in it is still the evidence:*
Option A: promote
`_old/external/pathways_2026/reference_proteins.{faa,tsv}` (54,005 KO-labelled KEGG gene
sequences) to `originals/kegg_proteins/` and build the pool here. This genuinely closes the
"cannot be derived here" gap, but produces a *different* pool from the staged scadc one
(273,764 rows), so the `pbert_transfer` lane's numbers move — and the sequences are
KEGG-sourced, the same licensing box as `kegg.requests_db`. Option B: keep the staged
scadc artifact as an unreproducible external import with provenance recorded; numbers stay
comparable to the deployed lane, gap stays open but stops being mysterious. Note that
`benchmark_v4` has its own pool work (`48_pbert_calibrate.py`, `49_pbert_verify_pool.py`,
`49b_pool_chain_check.py`), so porting B3 may settle this on its own.

**`functional_annotation_alt/esmc` (`esmc_600m.tgz`).** Needed only if an ESM-C or EZpred
lane is in the benchmark GPR. The chosen-4 lane set does not include one; `gpr_7lane` does.
(There is no `lanes.yml` and never was -- the lane sets are `LANE_SETS` in
`lib::fabfos_evidence.py`, read by both mappers.)
Delete unless the 7-lane mapper is on the benchmark path.

**`benchmark/v3/ground_truth/gof_*.tsv`.** These are the paper tables already extracted into
tabular form — the input to B3/B4 rather than an output. Under the tier rule they are
either hand-authored judgement with no producer -- which has no tier now that
`curated/` is gone -- or they get re-extracted from `originals/literature/` by a
transform. v4's cohort structure may supersede them entirely.
