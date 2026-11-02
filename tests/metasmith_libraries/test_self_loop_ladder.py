from collections import Counter

from metasmith.python_api import DEFERRED, DataInstanceLibrary, Spec, record_library

from conftest import MLIB

# seqkit_filter_contigs and polypolish each produce a type that extends the
# sequences::assembly they require, so either can consume the other's product
# without end. In curation round 6 this spec solved through an eleven-step
# filter/polish ladder (19 steps where 13 do the job). Nothing in the search
# forbids it; this pins that the search does not wander into it.
def test_mags_from_metagenome_filters_once_and_never_polishes(tmp_path):
    inputs = DataInstanceLibrary(tmp_path / "inputs.xgdb")
    for name in ("sequences", "ref", "annotation", "taxonomy", "binning", "alignment"):
        inputs.AddTypeLibrary(MLIB / "data_types" / f"{name}.yml")
    meta = inputs.AddValue("read_metadata.json", {"parity": "paired", "length_class": "short"},
                           "sequences::read_metadata")
    pair = inputs.AddValue("read_pair.txt", "sample_1", "sequences::read_pair", parents={meta})
    inputs.AddItem(DEFERRED, "sequences::zipped_forward_short_reads", parents={pair})
    inputs.AddItem(DEFERRED, "sequences::zipped_reverse_short_reads", parents={pair})
    inputs.AddValue("min_contig_length.txt", "2500", "sequences::min_contig_length")
    inputs.AddItem(DEFERRED, "ref::gtdb")

    task = Spec(
        input_library=record_library(inputs),
        sample_type=None,
        target_types=[
            "sequences::megahit_assembly",
            {"type": "sequences::filtered_assembly", "parents": [0]},
            {"type": "sequences::metabat2_bin_fasta", "parents": [1]},
            {"type": "taxonomy::checkm_stats", "parents": [2]},
            {"type": "taxonomy::gtdbtk", "parents": [2]},
            "sequences::fastqc_html_report",
            "sequences::fastp_json_report",
        ],
        transform_libraries=[MLIB / "transforms" / g for g in ("logistics", "assembly", "metagenomics", "kbase")],
        resource_libraries=[MLIB / "resources" / "env", MLIB / "resources" / "lib"],
    ).Solve()

    used = Counter(s.transform.name for s in task.plan.steps)
    assert task.ok, task.plan.dropped_targets
    assert used["seqkit_filter_contigs"] == 1, used
    assert used["polypolish"] == 0, used
    assert len(task.plan.steps) <= 13, used
