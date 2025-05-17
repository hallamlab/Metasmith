# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.1
# ---

# %% [markdown]
# Resolve common imports

# %%
from pathlib import Path
import subprocess
import shutil


# %% [markdown]
# # Run fastqc

# %%
base_file = Path().resolve()
data_dir = base_file / "sample_data"
subprocess.run(f"""
    mkdir -p {data_dir / "fastqc"} && \
    docker run --rm \
        -v {data_dir}:/data \
        -w /data \
        biocontainers/fastqc:v0.11.9_cv8 \
            fastqc --noextract \
                -o fastqc \
                dataset.fastq
""", shell=True, check=True)


# %% [markdown]
# Assert output files exist

# %%
output_dir = data_dir / "fastqc"
assert(output_dir.is_dir())

output_files = ["dataset_fastqc.html", "dataset_fastqc.zip"]
for f in output_files:
    f = output_dir / f
    assert(f.exists())


# %% [markdown]
# Clean up fastqc outputs

# %%
shutil.rmtree(output_dir)
print("\nFASTQC test passed!\n\n")



# %% [markdown]
# # Run longqc
#
# Note: `--user ...` is required since longqc by default makes the output directory owned by root

# %%
base_file = Path().resolve()
data_dir = base_file / "sample_data"
subprocess.run(f"""
    docker run --rm \
        --user "$(id -u):$(id -g)" \
        -v {data_dir}:/data \
        -w /data \
        cymbopogon/longqc:1.2.0 \
            sampleqc \
                -x pb-sequel \
                -o longqc \
                dataset.fastq
""", shell=True, check=True)


# %% [markdown]
# Assert output files exist

# %%
output_dir = data_dir / "longqc"
assert(output_dir.is_dir())

output_files = ["longqc_sdust.txt", "QC_vals_longQC_sampleqc.json", "web_summary.html"]
for f in output_files:
    f = output_dir / f
    assert(f.exists())

output_subdirs = ["analysis", "figs", "logs"]
for f in output_subdirs:
    f = output_dir / f
    assert(f.is_dir())


# %% [markdown]
# Clean up longqc outputs

# %%
shutil.rmtree(output_dir)
print("\nLONGQC test passed!\n\n")
