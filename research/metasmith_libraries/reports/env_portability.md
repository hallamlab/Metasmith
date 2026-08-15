# Tool-environment portability worklist

Which transforms can run on a host with no containers, and what stands in the
way of the rest. Regenerate with the metasmith engine's dispatch scan; the same
two facts drive `RunWorkflow`'s preflight, so this file and that refusal always
agree.

Two independent reasons a transform is not mamba-runnable, with different fixes:

* **no venv arm** — the transform declares only `ifContainerDo`. Fix by
  authoring `.ifVirtualEnvDo(env=..., cmd=...)` with the command that tool
  actually needs outside its image. Most of these pass `binds=`, meaning the
  container command reads a path that only exists inside the image; the venv
  command is genuinely different, not a copy.
* **`<tool>.env: no conda:`** — the tool has no conda form at all. No API change
  fixes this; either a recipe is added under `envs/tools/` or the tool stays
  container-only, which for image-internal layouts (interproscan's data dir,
  java installers) is the correct permanent answer.

    transforms declaring a tool env: 116
      mamba-runnable today:          53
      trivial venv arm available:    0  (no binds, conda: present)
      blocked:                       63
    
    --- trivially portable (needs only a venv arm authored) ---
    
    --- blocked ---
      transforms/amplicon/qiime2_taxonomy.py: no venv arm; qiime2.env: no conda:
      transforms/assembly/hifi/hifiasm.py: no venv arm; gfatools.env: no conda:
      transforms/assembly/hifi/hifiasm_meta.py: no venv arm; gfatools.env: no conda:
      transforms/fabfos/cluster_contigs.py: no venv arm; python_for_data_science.env: no conda:
      transforms/fabfos/gpr_4lane.py: no venv arm; python_for_data_science.env: no conda:
      transforms/fabfos/gpr_7lane.py: no venv arm; python_for_data_science.env: no conda:
      transforms/functionalAnnotation/ankh.py: no venv arm; ankh.env: no conda: ; passes binds
      transforms/functionalAnnotation/antismash.py: no venv arm ; passes binds
      transforms/functionalAnnotation/bakta_noncoding.py: no venv arm ; passes binds
      transforms/functionalAnnotation/busco.py: no venv arm ; passes binds
      transforms/functionalAnnotation/busco_full_table.py: no venv arm ; passes binds
      transforms/functionalAnnotation/clean.py: no venv arm; clean.env: no conda: ; passes binds
      transforms/functionalAnnotation/deepec.py: no venv arm; deepec.env: no conda:
      transforms/functionalAnnotation/deeptfactor.py: no venv arm; deeptfactor.env: no conda:
      transforms/functionalAnnotation/diamond_tcdb.py: no venv arm ; passes binds
      transforms/functionalAnnotation/diamond_tcdb2.py: no venv arm ; passes binds
      transforms/functionalAnnotation/diamond_uniref50.py: no venv arm ; passes binds
      transforms/functionalAnnotation/dram_annotate_genes.py: no venv arm ; passes binds
      transforms/functionalAnnotation/dramv.py: no venv arm ; passes binds
      transforms/functionalAnnotation/eggnog_mapper.py: no venv arm ; passes binds
      transforms/functionalAnnotation/esm_c.py: no venv arm; esmc.env: no conda: ; passes binds
      transforms/functionalAnnotation/esmfold.py: no venv arm; esmfold.env: no conda: ; passes binds
      transforms/functionalAnnotation/ezpred.py: no venv arm; ezpred.env: no conda: ; passes binds
      transforms/functionalAnnotation/foldseek_3di.py: no venv arm; foldseek.env: no conda:; polars.env: no conda: ; passes binds
      transforms/functionalAnnotation/interproscan.py: no venv arm; interproscan.env: no conda: ; passes binds
      transforms/functionalAnnotation/kofamscan.py: no venv arm ; passes binds
      transforms/functionalAnnotation/pathologic.py: no venv arm; pathologic.env: no conda: ; passes binds
      transforms/functionalAnnotation/predictf.py: no venv arm; predictf.env: no conda: ; passes binds
      transforms/functionalAnnotation/promotech.py: no venv arm; promotech.env: no conda:
      transforms/functionalAnnotation/proteinbert.py: no venv arm; proteinbert.env: no conda:; polars.env: no conda:
      transforms/functionalAnnotation/prott5.py: no venv arm; prott5.env: no conda: ; passes binds
      transforms/functionalAnnotation/ptools_annotation_gather.py: no venv arm; python_for_data_science.env: no conda:
      transforms/functionalAnnotation/saprot.py: no venv arm; saprot.env: no conda: ; passes binds
      transforms/functionalAnnotation/virsorter2.py: no venv arm ; passes binds
      transforms/logistics/downloadAnkhWeights.py: no venv arm; python_for_data_science.env: no conda:
      transforms/logistics/downloadCentrifugerDB.py: no venv arm; python_for_data_science.env: no conda:
      transforms/logistics/downloadESMCWeights.py: no venv arm; python_for_data_science.env: no conda:
      transforms/logistics/downloadESMFoldWeights.py: no venv arm; python_for_data_science.env: no conda:
      transforms/logistics/downloadInterProScanDB.py: no venv arm; python_for_data_science.env: no conda:; interproscan.env: no conda: ; passes binds
      transforms/logistics/downloadKofamDB.py: no venv arm; python_for_data_science.env: no conda:
      transforms/logistics/downloadKraken2DB.py: no venv arm; python_for_data_science.env: no conda:
      transforms/logistics/downloadProtT5XLWeights.py: no venv arm; python_for_data_science.env: no conda:
      transforms/logistics/downloadSaProtWeights.py: no venv arm; python_for_data_science.env: no conda:
      transforms/logistics/downloadSilvaDB.py: no venv arm; python_for_data_science.env: no conda:
      transforms/logistics/downloadSylphDB.py: no venv arm; python_for_data_science.env: no conda:
      transforms/logistics/dumpNcbiSra.py: no venv arm; sra-tools.env: no conda:
      transforms/logistics/getNcbiSra.py: no venv arm; sra-tools.env: no conda:
      transforms/logistics/ora2fastq.py: no venv arm; orad.env: no conda:
      transforms/metabolicModelling/differential_analysis.py: no venv arm; metabolomics-python.env: no conda:
      transforms/metabolicModelling/fba_constraint.py: no venv arm; metabolomics-python.env: no conda: ; passes binds
      transforms/metabolicModelling/jgi_loader.py: no venv arm; metabolomics-python.env: no conda: ; passes binds
      transforms/metabolicModelling/pathway_enrichment.py: no venv arm; metabolomics-python.env: no conda: ; passes binds
      transforms/metagenomics/taxonomy/centrifuger.py: no venv arm; python_for_data_science.env: no conda:
      transforms/metagenomics/taxonomy/gtdbtk.py: no venv arm ; passes binds
      transforms/metagenomics/taxonomy/kraken2.py: no venv arm; python_for_data_science.env: no conda:
      transforms/pangenome/heatmap.py: no venv arm; python_for_data_science.env: no conda: ; passes binds
      transforms/responseSurface/response_surface.py: no venv arm; python_for_data_science.env: no conda: ; passes binds
      transforms/transcriptomics/braker3.py: no venv arm; braker3.env: no conda:
      transforms/transcriptomics/genbank_to_reference.py: no venv arm; python_for_data_science.env: no conda:
      transforms/transcriptomics/organellar_count_matrix.py: no venv arm; python_for_data_science.env: no conda:
      transforms/transcriptomics/salmon_count_table.py: no venv arm; python_for_data_science.env: no conda:
      transforms/transcriptomics/stringtie_count_matrix.py: no venv arm; python_for_data_science.env: no conda:
      transforms/transcriptomics/volcano_plot.py: no venv arm; python_for_data_science.env: no conda:
    

## Not ported

Six transforms under `_disabled/` still call the retired `ExecWithContainer`
and were deliberately left alone, so re-enabling one is a two-line edit rather
than a silent `AttributeError`. `msm transform validate` names them:

    transforms/assembly/_disabled/fastp.py:20
    transforms/assembly/_disabled/fastqc.py:24
    transforms/assembly/_disabled/nanoplot.py:21
    transforms/assembly/_disabled/reads_to_assembly_bam.py:48,56
    transforms/logistics/_disabled/shardFasta.py:107
    transforms/metagenomics/binning/_disabled/checkm.py:30
