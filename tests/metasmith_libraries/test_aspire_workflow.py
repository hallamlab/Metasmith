import pytest

from metasmith.python_api import DEFERRED, Spec, TransformInstanceLibrary, record_library

from conftest import MLIB

PRESET = MLIB.parents[1] / "research" / "aspire" / "presets" / "aspire.yml"
SWITCHES = ("spieceasi", "network_modules", "asv_mag_link", "graph_network")
DEFAULT_ON = set(SWITCHES)
REFERENCES = ("aspire::mito_reference_source",
              "aspire::contaminant_reference_source", "amplicon::silva_db")
CORE = ["amplicon::asv_taxonomy", "aspire::counts_clean", "aspire::read_fate"]


@pytest.fixture(scope="module")
def aspire_transforms(mlib):
    return [
        TransformInstanceLibrary.Load(mlib / "transforms/aspire"),
        TransformInstanceLibrary.Load(mlib / "transforms/logistics"),
    ]


@pytest.fixture
def aspire_inputs(tmp_inputs):
    def _build(on=DEFAULT_ON, samples=2, parity="paired"):
        inputs = tmp_inputs(["aspire.yml", "amplicon.yml", "sequences.yml"])
        sheet = "sample\tlabel\n" + "".join(
            f"sample_{i}\t{'A' if i % 2 else 'B'}\n" for i in range(1, samples + 1))
        study = inputs.AddValue("study_metadata.tsv", sheet, "aspire::study_metadata")
        inputs.AddValue("params.yml", PRESET.read_text(), "aspire::params", parents={study})
        for i in range(1, samples + 1):
            meta = inputs.AddValue(f"read_metadata_{i}.json",
                                   {"sample": f"sample_{i}", "parity": parity,
                                    "length_class": "short"},
                                   "sequences::read_metadata", parents={study})
            if parity == "paired":
                pair = inputs.AddValue(f"read_pair_{i}.txt", f"sample_{i}",
                                       "sequences::read_pair", parents={meta})
                inputs.AddItem(DEFERRED, "sequences::zipped_forward_short_reads", parents={pair})
                inputs.AddItem(DEFERRED, "sequences::zipped_reverse_short_reads", parents={pair})
            else:
                inputs.AddItem(DEFERRED, "sequences::short_reads_se", parents={meta})

        for dtype in REFERENCES:
            inputs.AddItem(DEFERRED, dtype)

        for base in SWITCHES:
            arm = "on" if base in on else "off"
            inputs.AddValue(f"policy_{base}.txt", arm, f"aspire::{base}_{arm}",
                            parents={study})
        if "spieceasi" not in on:
            for dtype in ("external_graph_all", "external_graph_thr",
                          "external_node_features"):
                inputs.AddItem(DEFERRED, f"aspire::{dtype}")

        return record_library(inputs)
    return _build


def solve(inputs, transforms, targets):
    spec = Spec(
        input_library=inputs,
        target_types=list(targets),
        transform_libraries=transforms,
        resource_libraries=[MLIB / "resources" / "env", MLIB / "resources" / "lib"],
        # One study, one view. Splitting by sample would hand the collecting
        # transform one sample at a time.
        sample_type=None,
    )
    return spec.Solve()


def picked(task, transforms):
    names = {
        ti.model.key: (ti.name or str(path))
        for lib in transforms
        for path, ti in lib.IterateTransforms()
    }
    return [names.get(s.transform.model.key, "?") for s in task.plan.steps]


class TestAspireTopology:
    # A plan step fans out per sample at run time, so each row appears once.
    @pytest.mark.parametrize("parity, interleaves", [("paired", 1), ("single", 0)])
    def test_core_spine_solves(self, aspire_transforms, aspire_inputs, parity, interleaves):
        task = solve(aspire_inputs(samples=3, parity=parity), aspire_transforms, CORE)
        assert task.ok, f"[{parity}] core did not solve: dropped {sorted(task.plan.dropped_targets)}"
        steps = picked(task, aspire_transforms)
        assert steps.count("interleave_zipped_short_reads") == interleaves, steps
        for row in ("fastp_qc", "merge_and_filter_reads", "denoise"):
            assert steps.count(row) == 1, steps
        assert {"taxonomy", "curate", "read_accounting"} <= set(steps), steps

    # The reads are children of their metadata, which is a child of the sample sheet. A paired
    # half is three ancestors deep, the most a given can be and keep its read_pair link.
    def test_reads_descend_from_their_metadata(self, aspire_transforms):
        lib = aspire_transforms[0]
        fastp = next(ti for _p, ti in lib.IterateTransforms() if ti.name == "fastp_qc")

        def slot(dtype):
            want = lib.GetType(dtype).properties
            return next(r for r in fastp.model.requires if r.properties == want)

        reads, meta, study = (slot(t) for t in ("sequences::short_reads", "sequences::read_metadata",
                                                 "aspire::study_metadata"))
        assert reads.parents == {meta}
        assert meta.parents == {study}

    def test_master_summary_solves(self, aspire_transforms, aspire_inputs):
        task = solve(aspire_inputs(), aspire_transforms, ["aspire::master_long"])
        assert task.ok, f"master summary did not solve: dropped {sorted(task.plan.dropped_targets)}"
        steps = picked(task, aspire_transforms)
        assert {"master_summary", "sankey", "indicspecies"} <= set(steps), steps

    def test_solve_is_reproducible(self, aspire_transforms, aspire_inputs):
        inputs = aspire_inputs()
        targets = ["aspire::diversity_outputs", "aspire::clustermap_outputs"]
        a = solve(inputs, aspire_transforms, targets)
        b = solve(inputs, aspire_transforms, targets)
        assert a.ok and b.ok
        assert [s.transform.model.key for s in a.plan.steps] == \
               [s.transform.model.key for s in b.plan.steps]


@pytest.mark.parametrize("base, on_transform, off_transform, targets", [
    ("spieceasi", "spieceasi", "spieceasi_external",
     ["aspire::network_outputs"]),
    ("network_modules", "network_modules", "network_modules_absent",
     ["aspire::network_outputs"]),
    ("asv_mag_link", "asv_mag_link", "asv_mag_link_absent",
     ["aspire::network_outputs"]),
    ("graph_network", "graph_network", "graph_network_absent",
     ["aspire::master_long"]),
])
class TestPolicySwitches:
    def _arms(self, aspire_transforms, aspire_inputs, base, targets, enabled):
        on = (DEFAULT_ON | {base}) if enabled else (DEFAULT_ON - {base})
        task = solve(aspire_inputs(on=on), aspire_transforms, targets)
        assert task.ok, (
            f"[{base}={'on' if enabled else 'off'}] did not solve: "
            f"dropped {sorted(task.plan.dropped_targets)}"
        )
        return set(picked(task, aspire_transforms))

    def test_on_arm_selected(self, aspire_transforms, aspire_inputs,
                             base, on_transform, off_transform, targets):
        steps = self._arms(aspire_transforms, aspire_inputs, base, targets, True)
        assert on_transform in steps, steps
        assert off_transform not in steps, steps

    def test_off_arm_selected(self, aspire_transforms, aspire_inputs,
                              base, on_transform, off_transform, targets):
        steps = self._arms(aspire_transforms, aspire_inputs, base, targets, False)
        assert off_transform in steps, steps
        assert on_transform not in steps, steps
