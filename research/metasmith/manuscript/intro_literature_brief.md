# Literature brief — metasmith manuscript, introduction

## Purpose & Contents

Context for a literature-search agent tasked with finding citable work for the introduction of the
metasmith manuscript. It states what metasmith is, the argument the introduction makes, the
individual claims that need external support, and the search directions for each claim.

It does not contain the manuscript text, results, or methods. It does not contain a bibliography —
producing one is the reader's job.

The agent reading this has no access to the metasmith source tree. Everything it needs to judge
whether a paper is relevant is stated here.

## What metasmith is

Metasmith is a workflow generation system for bioinformatics. A user declares three things: the
data they already hold, the result they want, and the tools available. A planner searches backwards
from the result for a chain of tools connecting it to the data, compiles that chain to Nextflow,
and executes it on a local machine or a remote cluster. The user writes no workflow code.

The mechanism is a type system, not a script. A data type is a set of property strings. A tool
declares which properties its inputs must carry and which properties its outputs will carry.
"This output can fill that input" is a subset test over property sets. The planner is a search over
those tests, so adding a tool means writing one file that declares its inputs, outputs and
invocation, after which the planner uses that tool anywhere it helps. No existing workflow is
edited to accommodate a new tool.

Three properties distinguish it from a workflow language and matter to the introduction's argument:

1. **The workflow is derived, not written.** The unit of authorship is a single tool declaration.
   The pipeline is a search result.
2. **Tool installation is declared once per tool and is runtime-agnostic.** Each tool carries an
   optional container image pinned by digest and an optional conda environment. The engine picks by
   what the execution host offers. A tool declaration never branches on which one it got.
3. **Provenance is produced by construction.** Every output file records the input files the task
   actually read. The result of a run is a structured data product whose lineage is queryable, not
   a directory of files named by convention.

The current standard library carries 226 tool declarations across 51 pinned tool environments,
20 type namespaces, and 12 shipped end-to-end workflow templates covering assembly, annotation,
binning, pangenomics, amplicon/ASV surveys, viromics and metabolic modelling.

Two components are relevant to the reproducibility thread specifically. A task cache identifies
work by provenance — the transform, the lineage, and the identities of the files consumed — rather
than by output bytes, so reuse across runs is decided by what a step was asked to do. And the
planner's output is adjudicated by a separately specified checker: the specification of what makes
a plan correct is written in Lean 4, and the checker that decides it is extracted from that
specification rather than written by hand. The search itself is not verified. The answer is.

## The argument the introduction makes

The introduction moves from a problem to a shape of solution in three threads, then situates the
work against existing systems. The threads are stated below as numbered claims. Each claim needs
external support, and the note under each says what kind of support would actually settle it.

**Distinguish two failure modes when judging a candidate paper.** A paper that *asserts* a problem
in its own introduction is weak support and should be recorded as such. A paper that *measures* the
problem — a re-execution rate, an install-failure rate, a survey with an n, a longitudinal audit —
is strong support and is what this search is primarily for. Prefer the second everywhere. Where
only the first exists for a claim, say so explicitly, because that changes how the claim gets
written.

---

## Thread 1 — Accessibility of running bioinformatics analysis

The problem: producing a result from sequencing data requires composing many separate tools,
expressing that composition in a workflow language, and understanding the compute infrastructure it
runs on. That skill set is distinct from the biology that motivates the question, and the people
holding the question frequently do not hold the skill set.

### Claims needing support

- **C1.1** — Sequence data generation has outpaced the capacity to analyse it. Wanted: figures on
  sequencing cost and volume growth against analyst or trained-personnel supply. The cost-per-genome
  curve is well documented and easy. The *analysis capacity* half is the hard half and the more
  valuable find.
- **C1.2** — Bioinformatics training is a recognised and quantified bottleneck for wet-lab
  researchers. Wanted: surveys of researchers reporting analysis as a barrier, training-needs
  assessments, papers on the bioinformatics skills gap with sample sizes. Look for work from
  training consortia and from journals that publish community surveys.
- **C1.3** — Writing and maintaining workflow code is itself a specialist skill with a real learning
  cost. Wanted: studies of workflow language adoption, usability studies of workflow systems,
  developer-experience surveys of pipeline authors. Anything measuring how long it takes someone to
  become productive in a workflow language is gold.
- **C1.4** — Graphical and no-code interfaces materially widen who can run an analysis, and existing
  ones trade breadth for control. Wanted: the Galaxy platform literature (its motivation papers are
  explicitly about accessibility for non-programmers), plus any evaluation of what such platforms do
  and do not cover. Also worth finding: critiques of point-and-click platforms, since the manuscript
  will need to say what metasmith does differently rather than claiming the problem is unaddressed.
- **C1.5** — Adding a tool to an existing pipeline is disproportionately expensive relative to the
  size of the change. This is the claim the type-directed design answers directly. Wanted: anything
  measuring pipeline maintenance burden, coupling in workflow code, or the cost of extending
  community pipelines. This may be thin. Report honestly if it is.

### Search directions

Terms: bioinformatics skills gap; bioinformatics training needs survey; computational biology
workforce; barriers to data analysis wet lab researchers; workflow system usability; no-code
bioinformatics; democratizing genomic data analysis; Galaxy platform accessibility; sequencing data
deluge analysis bottleneck.

Anchors to confirm and retrieve (leads, not verified citations): the Galaxy platform papers and
their biennial update series; ELIXIR and Carpentries training-landscape reports; community surveys
on bioinformatics capacity in *Briefings in Bioinformatics*, *PLOS Computational Biology* (the
"Ten Simple Rules" and education collections), and *GigaScience*.

---

## Thread 2 — Technical churn of tool installation

The problem: bioinformatics tools are numerous, independently maintained, mutually incompatible in
their dependencies, and unstable over time. Assembling the ten to twenty tools a real analysis needs
is a substantial engineering task that recurs on every new machine, and a working installation
decays without anyone touching it.

### Claims needing support

- **C2.1** — The bioinformatics tool ecosystem is large, fragmented, and growing. Wanted: counts of
  registered tools and their growth rate over time, and the rate at which new tools are published.
  Registry-level numbers with dates attached are what to look for.
- **C2.2** — A substantial fraction of published bioinformatics software cannot be installed or run
  by a third party. Wanted: audits that attempted installation at scale and reported a failure rate.
  This is the single most valuable find in this thread. Search for software-availability audits,
  link-rot studies of tool URLs, and reproducibility audits that report *why* re-execution failed,
  since installation failure is usually the dominant reported cause.
- **C2.3** — Dependency conflict between tools is a routine and structural obstacle, not an
  occasional accident. Wanted: anything characterising dependency resolution problems in scientific
  software, including package-manager-level analyses.
- **C2.4** — Containers and package channels are the field's current answer, they work, and they
  are not free. Wanted: the Bioconda and BioContainers literature, container adoption studies in
  bioinformatics, and — importantly — papers reporting the *limits* of that answer: image drift,
  unpinned or mutable tags resolving to different contents over time, image size and registry
  availability, and the cost of maintaining recipes. The manuscript's position is that this layer is
  necessary and that metasmith consumes it rather than replacing it, so balanced evidence is right.
- **C2.5** — Software decays without modification, through its environment moving underneath it.
  Wanted: longitudinal studies re-running old software, "software rot" or "software collapse"
  literature, and any work measuring how quickly an untouched computational environment stops
  reproducing.

### Search directions

Terms: bioinformatics software availability; scientific software installability; dependency hell
scientific software; Bioconda; BioContainers; container adoption bioinformatics; software collapse;
software rot computational science; reproducible software environments; conda environment
resolution; docker image drift reproducibility.

Anchors to confirm and retrieve (leads, not verified citations): the Bioconda paper (Grüning et al.,
*Nature Methods*); the BioContainers paper (da Veiga Leprevost et al., *Bioinformatics*); Konrad
Hinsen's writing on software collapse; bio.tools / EDAM registry papers for the ecosystem-size
claim; software-availability audits published in *Bioinformatics*, *BMC Bioinformatics* and
*GigaScience*.

---

## Thread 3 — Reproducibility and provenance

The problem: a published computational analysis is frequently not re-runnable, and even when the
outputs are available their derivation is not recorded. Provenance — which input produced which
output through which tool version — is usually reconstructed after the fact from file names and
directory layout, which is a convention rather than a record.

This thread is where the manuscript's technical contribution is sharpest, so the claims are more
specific.

### Claims needing support

- **C3.1** — Published computational analyses frequently fail to re-execute. Wanted: re-execution
  studies with a denominator. Papers that took n published analyses, attempted to reproduce them,
  and reported how many succeeded and why the rest failed. Cross-domain studies count. Report the
  numbers and the failure-cause breakdown, because the breakdown is what connects this thread to
  Thread 2.
- **C3.2** — Reproducibility is not the same as provenance, and workflow systems mostly deliver the
  first. Wanted: work distinguishing re-execution from lineage capture, and any critique of workflow
  systems on provenance grounds. The distinction the manuscript draws: re-running a pipeline proves
  the pipeline runs, and answers nothing about which of forty output files descends from which of
  eleven samples.
- **C3.3** — Provenance standards and models exist and are underused in practice. Wanted: W3C PROV
  and its bioinformatics-facing profiles, RO-Crate and Workflow RO-Crate, and any study measuring
  actual adoption. The manuscript needs to place its own lineage record against these — metasmith
  records lineage per file, produced by the task that made the file, rather than declared by the
  workflow author — so both the standards and their uptake matter.
- **C3.4** — FAIR principles apply to workflows and software, not only to data. Wanted: the FAIR
  principles paper, FAIR-for-research-software, and FAIR computational workflows work.
- **C3.5** — Naming conventions and directory layout are load-bearing but unreliable carriers of
  provenance in practice. Wanted: anything documenting sample mix-ups, mislabelling, or metadata
  loss in sequencing analyses. Concrete incident reports are more persuasive here than
  methodological argument, if any exist.
- **C3.6** — Verified checking of a search result is an established alternative to verifying the
  search. Wanted: the certificate-checking literature outside bioinformatics — DRAT and its
  predecessors for SAT solving, Farkas certificates for linear programming, verified checkers
  generally, and proof-carrying results. Also wanted: any prior use of machine-checked
  specifications in bioinformatics tooling, which is expected to be sparse and is worth confirming
  as sparse.
- **C3.7** — Caching and incremental re-execution in workflow systems are keyed on file content or
  timestamps, with known failure modes. Wanted: descriptions of how existing systems decide reuse,
  and any analysis of when that decision is wrong. Metasmith keys reuse on provenance rather than on
  output bytes, and the introduction should say what that fixes.

### Search directions

Terms: computational reproducibility study re-execution rate; reproducibility crisis computational
biology; workflow provenance capture; W3C PROV; RO-Crate workflow; FAIR computational workflows;
FAIR research software; data lineage scientific workflows; sample swap sequencing metadata;
certified checker; DRAT proof SAT solver; Farkas certificate; formally verified checker; proof
carrying code.

Anchors to confirm and retrieve (leads, not verified citations): the FAIR Guiding Principles paper
(Wilkinson et al., *Scientific Data*, 2016); FAIR Principles for Research Software (FAIR4RS);
RO-Crate and Workflow RO-Crate papers; W3C PROV-DM; large-scale reproducibility studies in
*PLOS ONE*, *PeerJ CS* and *Nature*; the DRAT-trim and verified-SAT-checker literature.

---

## Thread 4 — Existing systems, and the honest comparison

The introduction must place metasmith among existing workflow systems rather than imply the space is
empty. Retrieve the primary reference for each system below plus, where it exists, an independent
comparison or evaluation of them. Comparative evaluations by third parties are worth more than each
system's own paper and are the harder find.

Systems to retrieve primary citations for:

- **Workflow engines and languages**: Nextflow, Snakemake, Common Workflow Language (CWL), Workflow
  Description Language (WDL), Cromwell, Toil, Galaxy, Bpipe, Luigi, Airflow where used scientifically.
- **Curated pipeline collections**: nf-core, and any analysis of what curated collections solve and
  what they cost.
- **Registries and ontologies**: bio.tools, EDAM, WorkflowHub, Dockstore.
- **Automated workflow composition in bioinformatics** — this is the closest prior art and the most
  important retrieval in the whole brief. Metasmith is not the first system to synthesise a
  bioinformatics workflow from typed tool descriptions. Search hard here. Terms: automated workflow
  composition bioinformatics; workflow synthesis semantic types; loose programming; APE Automated
  Pipeline Explorer; PROPHETS; semantic web service composition bioinformatics; BioMOBY; Taverna;
  temporal-logic workflow synthesis. For each system found, record what its type model was, what its
  search was, whether it executed the workflows it synthesised or only proposed them, and whether it
  is still maintained. That comparison is the substance of the related-work discussion.

**What the comparison must be able to say.** Existing workflow engines are languages for *expressing*
a pipeline the author already has in mind — metasmith produces the pipeline. Existing composition
systems produced a plan — metasmith compiles the plan onto an execution engine and runs it on real
clusters with real provenance. The retrieved literature has to be strong enough to support or refute
both halves. Refuting either is a useful outcome and should be reported plainly rather than softened.

---

## Thread 5 — Method lineage outside bioinformatics

Supporting citations for the machinery, needed in smaller volume. One or two authoritative
references per topic is enough.

- **Backward search / goal-directed planning**: classical AI planning, backward chaining, and
  regression planning references.
- **Monte Carlo tree search with PUCT selection**: the AlphaGo/AlphaZero line for the selection rule,
  and the original UCT paper.
- **Type-directed program synthesis**: component-based synthesis and API/type-driven search, where a
  program is found by connecting components whose types compose. This is the computer-science framing
  of exactly what the planner does, and naming it correctly strengthens the paper.
- **Subtyping as set containment**: structural and property-based subtyping, for the "more properties
  means more specific" rule.
- **Lean 4 and proof extraction**: the Lean 4 system reference, and precedent for extracting an
  executable checker from a machine-checked specification.

---

## Scope fences

Do not retrieve:

- Biological results papers from the domains the shipped templates cover. Metagenomics, viromics and
  pangenomics results are not the subject. The exception is a citation needed to justify that a
  worked example is a real analysis someone runs, and those will be requested separately.
- Individual tool papers for the 226 declared tools. Those belong in the methods and the supplement,
  and will be gathered from the tool declarations rather than by search.
- General machine learning literature. The planner uses MCTS. That is the only intersection.
- Cloud infrastructure and container runtime engineering, beyond what C2.4 asks for.

## What to return

Return one entry per paper, grouped by the claim it supports, in this form:

- **Claim ID** it supports, and whether it is *strong* (measures the problem) or *weak* (asserts the
  problem).
- Full citation with DOI.
- One or two sentences stating what the paper actually shows. Include the number if there is one —
  the n, the failure rate, the year range. A summary of the abstract is not useful. What is useful
  is the sentence the manuscript could cite.
- A confidence note where the paper only partly supports the claim, saying which part.

Flag every claim for which nothing strong was found. A claim with no measurement behind it gets
written differently, and knowing which ones those are is as valuable as the citations themselves.

Rank the findings for Thread 4's automated-composition search separately and lead with them. If a
system exists that already does what metasmith does, that finding outranks everything else in this
brief.
