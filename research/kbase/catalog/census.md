# KBase census

The fixed denominator for this port, written at scrape time on 2026-09-04 before any conversion decision. `census.json` is the machine-readable
form and wins over this file if the two disagree.

### Apps

| | |
|---|---|
| total | 493 |
| active | 235 |
| inactive | 258 |
| runnable | 431 |
| viewer | 62 |
| with_listing_types | 478 |
| with_typed_inputs | 444 |
| with_typed_outputs | 266 |
| with_untyped_output_only | 8 |
| with_report_output | 298 |
| with_any_output_channel | 372 |
| with_no_output_channel | 121 |
| by_output_channel | typed+report: 192, none: 121, report: 106, typed: 74 |
| with_module | 422 |
| legacy_no_module | 71 |
| with_docker_image | 422 |
| referencing_a_dead_type | 48 |

### Active apps

| | |
|---|---|
| total | 235 |
| runnable | 226 |
| viewer | 9 |
| with_typed_outputs | 138 |
| with_report_output | 207 |
| with_any_output_channel | 217 |
| with_no_output_channel | 18 |
| with_docker_image | 232 |

### Workspace types

| | |
|---|---|
| referenced | 121 |
| resolved | 106 |
| unresolved | 15 |
| unresolved_by_reason | malformed_reference: 2, retired_module: 6, retired_type: 7 |

### Catalog modules

| | |
|---|---|
| total | 137 |
| with_git_url | 137 |
| with_docker_image | 137 |
| by_host | github.com: 135, gitlab.com: 2 |
| by_org | kbaseapps: 109, kbase: 6, bolduc: 4, jfroula: 2, sjyoo: 1, aekazakov: 1, jayrbolton: 1, dcchivian: 1, jeffkimbrel: 1, ModelSEED: 1, zahmeeth: 1, landml: 1, jjacobson95: 1, mikacashman: 1, bio-boris: 1, OGalOz: 1, LANL-Bioinformatics: 1, psdehal: 1, janakagithub: 1, kbaseIncubator: 1 |
