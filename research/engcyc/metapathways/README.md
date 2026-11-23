# MetaPathways transforms: fir pilot

## Purpose & Contents

This directory proves the MetaPathways transforms on fir before the Engcyc lead targets them. This file gives the run order and the fir facts the scripts depend on but cannot show. `run_pilot.py --help` lists the samples and subcommands. Results go to the journal, not here.

## Run order

1. Run `fetch_refs.sh /scratch/phyberos/engcyc/mp_pilot/refs` on a login node. Compute nodes have no internet.
2. Submit `index_ssu.sbatch` for the SSU BLAST index.
3. Run `sync_pilot.sh` from the workstation. It prints the fir checkout.
4. Run `run_pilot.py setup --deploy`, then `import --sample S` through the bench `submit_driver.sbatch`.
5. Run `run_pilot.py run --sample S --materialise` from the workstation.
6. Submit `run_pilot.py run --sample S --launch` through `submit_driver.sbatch`.

**CAUTION** Run step 5 before every launch that adds a tool. The workstation's agent shell is a fir login node, and compute nodes cannot reach quay. A launch with an image missing from the store fails at the first task that needs it.

**CAUTION** Run one importing driver per agent home at a time. Start drivers at least 3 min apart.

## Host facts

**WARNING** `pathologic.sif` is the licensed Pathway Tools 27.0 image. Never push or share it, and never loosen a mode on any copy.

- The lasting copy is `/project/rpp-shallam/phyberos/licensed/pathway_tools_27.0/`, mode 700, with the installer it was built from. Scratch purges do not reach it.
- `/scratch/phyberos/engcyc/licensed/` hardlinks both files on scratch. `setup` hardlinks the SIF from there into the home's mode-700 `container_images`.
- After a scratch purge, hardlink or copy from the project copy back into `/scratch/phyberos/engcyc/licensed/`.

- The agent home lives on scratch. A scratch purge loses its imported identities, so the next run re-imports every given.
- The fir Metabuli database (`/home/phyberos/project-rpp/lib/metabuli/gtdb`) is GTDB 214.1 and ships no `names.dmp` or `nodes.dmp`. `contig_lcastar` then resolves a Metabuli match taxID only when some contig was called to it, and its lineages are in the 214 frame. The only GTDB r232 Metabuli database is the Arbutus volume `metabuli-ref-gtdb232`.
- Nextflow's `peak_rss` measures the relay wrapper, not the tool. Read memory from `sacct -o MaxRSS` on the task's job.
