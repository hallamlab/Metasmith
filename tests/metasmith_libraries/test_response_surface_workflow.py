"""
End-to-end tests for the response surface media optimization transform.

These tests verify that the response surface workflow can be:
1. Generated (workflow planning)
2. Staged to the agent
3. Executed via local Docker
4. Produce valid CSV and SVG outputs
"""
import pytest
from pathlib import Path
from metasmith.python_api import (
    DataInstanceLibrary,
    TransformInstanceLibrary,
    TargetBuilder,
    Resources,
    Size,
    Duration,
)

from conftest import (
    wait_for_workflow,
    MLIB,
    TEST_DATA_DIR,
)


# ── Test data file lists ─────────────────────────────────────────────────────

DSE_FILES = [
    "R2-2_ReDecoded_Data_444.csv",
    "R2-2_ReDecoded_Data_495.csv",
    "R2-2_ReDecoded_Data_634.csv",
    "R2-2_ReDecoded_Data_684.csv",
]

AXIS_ALIGNED_FILES = [
    "R4-1_ReDecoded_Data_444.csv",
    "R4-1_ReDecoded_Data_495.csv",
    "R4-1_ReDecoded_Data_634.csv",
    "R4-1_ReDecoded_Data_684.csv",
]

ALL_FILES = [
    ("dse", f) for f in DSE_FILES
] + [
    ("axis_aligned", f) for f in AXIS_ALIGNED_FILES
]

ALL_FILE_IDS = [
    f"{fmt}-{Path(f).stem}" for fmt, f in ALL_FILES
]

# All expected output types
OUTPUT_TYPES = [
    "media_optimization::response_surface_coefficients",
    "media_optimization::model_suggestions",
    "media_optimization::crashed_cultures",
    "media_optimization::growth_characteristics_plot",
    "media_optimization::factor_importance_plot",
    "media_optimization::model_suggestions_plot",
    "media_optimization::response_surface_1d_plot",
    "media_optimization::response_surface_2d_plot",
]


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def response_surface_transforms(mlib):
    """Load response surface transforms."""
    return [
        TransformInstanceLibrary.Load(mlib / "transforms/responseSurface"),
    ]


@pytest.fixture
def media_opt_input(tmp_inputs, test_data_dir, request):
    """Create input library for a media optimization CSV file.

    Parametrized via indirect with (format_type, filename) tuples.
    """
    format_type, filename = request.param
    csv_path = test_data_dir / "media_opt" / "data" / format_type / filename

    if not csv_path.exists():
        pytest.skip(f"Test data not available: {csv_path}")

    inputs = tmp_inputs(["media_optimization.yml"])
    inputs.AddItem(csv_path, "media_optimization::growth_data")
    inputs.Save()

    return inputs


# ── Planning tests ───────────────────────────────────────────────────────────

class TestResponseSurfaceWorkflowGeneration:
    """Tests for workflow generation (planning only)."""

    @pytest.mark.parametrize("media_opt_input", ALL_FILES, ids=ALL_FILE_IDS, indirect=True)
    def test_can_plan_response_surface_workflow(
        self, agent, base_resources, lib_resources, response_surface_transforms, media_opt_input
    ):
        """Verify workflow generation for response surface analysis."""
        targets = TargetBuilder()
        targets.Add("media_optimization::response_surface_coefficients")

        resources = base_resources + [media_opt_input]
        if lib_resources:
            resources.append(lib_resources)

        task = agent.GenerateWorkflow(
            samples=list(media_opt_input.AsSamples("media_optimization::growth_data")),
            resources=resources,
            transforms=response_surface_transforms,
            targets=targets,
        )

        assert task.ok, f"Workflow generation failed: {task}"


# ── Execution tests ──────────────────────────────────────────────────────────

@pytest.mark.slow
class TestResponseSurfaceWorkflowExecution:
    """Full E2E tests that execute workflows via Docker."""

    @pytest.mark.parametrize("media_opt_input", ALL_FILES, ids=ALL_FILE_IDS, indirect=True)
    def test_response_surface_e2e(
        self, agent, base_resources, lib_resources, response_surface_transforms, media_opt_input
    ):
        """Full E2E test: stage, run response surface analysis, verify outputs."""
        targets = TargetBuilder()
        for output_type in OUTPUT_TYPES:
            targets.Add(output_type)

        resources = base_resources + [media_opt_input]
        if lib_resources:
            resources.append(lib_resources)

        task = agent.GenerateWorkflow(
            samples=list(media_opt_input.AsSamples("media_optimization::growth_data")),
            resources=resources,
            transforms=response_surface_transforms,
            targets=targets,
        )
        assert task.ok, f"Workflow generation failed: {task}"

        # Stage workflow
        agent.StageWorkflow(task, on_exist="clear")

        # Run workflow
        agent.RunWorkflow(
            task,
            config_file=agent.GetNxfConfigPresets()["local"],
            params=dict(
                executor=dict(cpus=2, queueSize=1),
                process=dict(tries=1),
            ),
            resource_overrides={
                "*": Resources(cpus=2, memory=Size.GB(8)),
            },
        )

        # Wait and verify
        results = wait_for_workflow(agent, task, timeout=600)
        results_path = agent.GetResultSource(task).GetPath()

        found_types = set()
        for path, type_name, endpoint in results.Iterate():
            for expected_type in OUTPUT_TYPES:
                short_type = expected_type.split("::")[-1]
                if short_type in type_name:
                    found_types.add(expected_type)

                    if not path.is_absolute():
                        full_path = results_path / path
                    else:
                        full_path = path

                    assert full_path.exists(), f"Output file missing: {full_path}"
                    assert full_path.stat().st_size > 0, f"Output file empty: {full_path}"

                    # Verify CSV files have headers
                    if full_path.suffix == ".csv":
                        content = full_path.read_text()
                        lines = content.strip().split("\n")
                        assert len(lines) >= 2, f"CSV should have header + data: {full_path}"

        # Check all expected output types were produced
        missing = set(OUTPUT_TYPES) - found_types
        assert not missing, f"Missing output types: {missing}"
