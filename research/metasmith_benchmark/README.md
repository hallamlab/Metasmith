# metasmith benchmark

## Purpose & Contents

This folder is the workspace for the five benchmark experiments: E1 nf-core/mag on CAMI, E2 the same pipeline driven by metasmith, E3 Pratama 2026, E4 metaGEM, and E5 the optimized pipeline. It holds the design page, the drivers for the five runs, and the findings they rest on.

- `PLAN.md` is the current plan: decisions taken, open decisions and the task list.
- `page/` builds the "Five Benchmark Experiments" artifact (https://claude.ai/code/artifact/d2fbf93b-4f88-4428-9a7b-ecd0e25ab14a).
- `findings/` holds research support's ledgers and the design findings that sit in neither.

## Build and publish the page

1. Edit `page/experiments.src.html` for prose and `page/tool_table.py` for the E5 tool table.
2. Put rendered DAGs in `page/dags/<key>.dag.svg` and reference them as `{{SVG:<key>}}`.
3. Run `python3 page/build.py`. It writes `page/experiments.html`, which git ignores.
4. Publish `page/experiments.html` with the Artifact tool, passing the artifact URL above as `url`.

CAUTION: publishing a different file path without `url` creates a new artifact instead of updating the existing one.
