# Globus transfer log — CAMI long-read + CAMI III restore

Source resolved to chinook (guest collection `2602486c-1e0f-47a0-be15-eec1b0ff0f96`,
`ubcarc#chinook` host, root `/Resources/external_data/CAMI/`), not sockeye. Sockeye
(`ubcarc#sockeye`, `64a5c402-05c4-4607-bbad-46a9c2aebd98`) was a transient relay for
the original Wasabi/de.NBI ingest and holds no persistent copy of this corpus.
Confirmed by directly listing `2602486c-...:/Resources/external_data/CAMI/` — the
five dataset subtrees named in the engine/cami journal are present there.

Destination: fir (`8dec4129-9ab4-451d-a45f-5b4b8471f7a3`, "computecanada#cedar-globus
& alliancecan#fir-globus"), under `/scratch/phyberos/cami/`.

Scope: long-read subtrees for CAMI II marine, strain, plant_associated (both nano and
pacbio platforms), plus both platforms of CAMI III toy_humangut. Short-read CAMI II
data does not need restoring — it is already unpacked on fir under
`/scratch/phyberos/cami/work/*_short_read/`.

Measured via `globus ls -r --recursive-depth-limit 4 -F json` (explicit depth, not
the default-3 that has previously undercounted): 632 files, 1,263,079,489,454 bytes
(1.263 TB) — smaller than the plan's ~2.4 TB estimate, which likely assumed short-read
CAMI II would need restoring too.

| Task ID | Submitted | Status |
|---|---|---|
| 86cf553e-ae97-11f1-a04b-0ee7ef9370d9 | 2026-09-12T03:5x UTC | submitted |

Poll: `mamba run -n globus globus task show <task id>`
