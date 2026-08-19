import pytest

from metasmith.python_api import DEFERRED, Spec, TransformInstanceLibrary

from conftest import MLIB

SWITCHES = ("augmentation", "batch_correction", "indicspecies", "spieceasi",
            "network_modules", "asv_mag_link", "graph_network", "sankey")
DEFAULT_ON = {"indicspecies", "spieceasi", "network_modules", "asv_mag_link",
              "graph_network", "sankey"}


@pytest.fixture(scope="module")
def aspire_transforms(mlib):
    return [
        TransformInstanceLibrary.Load(mlib / "transforms/aspire"),
        TransformInstanceLibrary.Load(mlib / "transforms/logistics"),
    ]


@pytest.fixture
def aspire_inputs(tmp_inputs):
    def _build(on=DEFAULT_ON, samples=2):
        inputs = tmp_inputs(["aspire.yml", "amplicon.yml", "sequences.yml"])
        run = inputs.AddValue("run.txt", "test_study", "aspire::run")
        for i in range(1, samples + 1):
            sid = inputs.AddValue(f"sample_{i}.txt", f"sample_{i}",
                                  "aspire::sample_id", parents={run})
            pair = inputs.AddValue(f"read_pair_{i}.txt", f"sample_{i}",
                                   "sequences::read_pair", parents={sid})
            inputs.AddItem(DEFERRED, "sequences::zipped_forward_short_reads", parents={pair})
            inputs.AddItem(DEFERRED, "sequences::zipped_reverse_short_reads", parents={pair})

        for dtype in ("aspire::sample_metadata", "aspire::sina_arb_reference",
                      "aspire::silva_ref_taxonomy", "aspire::mito_reference_source",
                      "aspire::contaminant_reference_source", "amplicon::silva_db"):
            inputs.AddItem(DEFERRED, dtype)

        for base in SWITCHES:
            arm = "on" if base in on else "off"
            inputs.AddValue(f"policy_{base}.txt", arm, f"aspire::{base}_{arm}",
                            parents={run})
        if "spieceasi" not in on:
            for dtype in ("external_graph_all", "external_graph_thr",
                          "external_node_features"):
                inputs.AddItem(DEFERRED, f"aspire::{dtype}")

        inputs.Save()
        return inputs
    return _build


def solve(inputs, transforms, targets):
    spec = Spec(
        input_library=inputs,
        target_types=list(targets),
        transform_libraries=transforms,
        resource_libraries=[MLIB / "resources" / "env"],
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
    return {names.get(s.transform.model.key, "?") for s in task.plan.steps}


class TestAspireTopology:
    def test_core_spine_solves(self, aspire_transforms, aspire_inputs):
        task = solve(aspire_inputs(), aspire_transforms,
                     ["amplicon::asv_taxonomy", "aspire::counts_filtered"])
        assert task.ok, f"core spine did not solve: dropped {sorted(task.plan.dropped_targets)}"
        steps = picked(task, aspire_transforms)
        assert {"fastp_qc", "concat_fastas", "create_count_matrix", "taxonomy",
                "filter_counts"} <= steps, steps

    def test_master_summary_solves(self, aspire_transforms, aspire_inputs):
        task = solve(aspire_inputs(), aspire_transforms, ["aspire::master_long"])
        assert task.ok, f"master summary did not solve: dropped {sorted(task.plan.dropped_targets)}"
        assert "master_summary" in picked(task, aspire_transforms)

    def test_solve_is_reproducible(self, aspire_transforms, aspire_inputs):
        inputs = aspire_inputs()
        targets = ["aspire::diversity_outputs", "aspire::clustermap_outputs"]
        a = solve(inputs, aspire_transforms, targets)
        b = solve(inputs, aspire_transforms, targets)
        assert a.ok and b.ok
        assert [s.transform.model.key for s in a.plan.steps] == \
               [s.transform.model.key for s in b.plan.steps]


@pytest.mark.parametrize("base, on_transform, off_transform, targets", [
    ("augmentation", "group_label_augmentation", "augmentation_passthrough",
     ["aspire::analysis_metadata"]),
    ("batch_correction", "asv_batch_correction", "batch_correction_passthrough",
     ["aspire::analysis_counts"]),
    ("indicspecies", "indicspecies", "indicspecies_absent",
     ["aspire::clustermap_outputs"]),
    ("spieceasi", "spieceasi", "spieceasi_external",
     ["aspire::network_outputs"]),
    ("network_modules", "network_modules", "network_modules_absent",
     ["aspire::network_outputs"]),
    ("asv_mag_link", "asv_mag_link", "asv_mag_link_absent",
     ["aspire::network_outputs"]),
    ("graph_network", "graph_network", "graph_network_absent",
     ["aspire::master_long"]),
    ("sankey", "sankey", "sankey_absent",
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
        return picked(task, aspire_transforms)

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
