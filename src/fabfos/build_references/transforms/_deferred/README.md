# Deferred — written, not in the library

A transform here is complete enough to read and wrong to load: it references
something that does not exist yet, so `build.sh` would fail on it and every gate
with it. `_deferred/` is not passed to `--transforms`, so nothing here is planned
against.

## `ezpred_model.py` — blocked on how patched source enters the graph

R9 assembles `ref::ezpred_model` from two halves: the Zenodo artifacts
(`acquire/ezpred.py`, done — models.zip + Data2.zip, byte-for-byte as served) and
the EZpred **source tree**. The second half has no home yet, and the reason is the
tier rule rather than an oversight.

Our copy of the tree is *patched* — the DL-only fork: no MMseqs2 homolog
augmentation, no Foldseek template fusion, a `--dl-only` flag on `predict.py`. A
patched tree is not what any URL returns, so it cannot sit in `originals/`, whose
whole contract is fidelity to the source. And it is a 77 MB directory, which does
not fit `buildlib::`'s flat one-module-per-file shape.

The two candidate resolutions, neither taken:

- **acquire the unpatched upstream at a pinned revision, and carry the patch as a
  `buildlib::` resource.** Correct by the tier rule and the smaller artifact, but it
  needs the patch to exist as a diff against a named upstream sha — and the vendored
  tree in `metasmith-libraries/deep-learning/resources/EZpred/` was edited in place,
  so that diff has to be reconstructed, not exported.
- **declare a directory-typed `buildlib::` resource** and vendor the patched tree.
  Simpler, but puts 77 MB of third-party source in this repo and makes "which
  upstream revision is this" a claim in a README rather than a fact in the graph.

Until one is chosen, `ref::ezpred_model` has no producer in the graph.

**The lane runs anyway, from a staged given.** The assembled bundle — the patched
tree with `models/{enzyme,nonenzyme}/EC/` in place, five members per head — sits at
`<remote-processed>/ezpred_model/EZpred` and `examples/scadc_gpr.py` declares it in
`REFS_7` at that absolute path. So the decision above gates *reproducing* the bundle
on a clean machine, not *using* it: the artifact is pinned and the lane's numbers are
real, but which upstream revision the patch applies to is still a claim in a README
rather than a fact in the graph. That is the whole of what is owed.

Two details this file had wrong, found by actually running it:

- **The IA tables are not needed.** `Data2.zip` is unpacked for `ia_file1`/`ia_file2`,
  and the `--dl-only` path never reads them — `predict.py`'s `InterLabelGO_pipeline`
  touches `MODEL_CHECKPOINT_DIR{1,2}` and nothing else under `Data/`. The transform
  above asserts on files its own consumer does not use.
- **The tree is 668 MB, not 77 MB**, and 605 MB of that is `models/` — which comes
  from `models.zip`, i.e. the half that *is* already acquirable. The source half is
  ~28 MB, almost all of it `utils/` binaries. The "77 MB directory does not fit
  `buildlib::`" objection is smaller than it was written to be.
