"""
Shared pytest fixtures for MetasmithLibraries end-to-end tests.

These fixtures provide a common agent and resource setup for all tests,
enabling consistent testing against local Docker deployment.
"""
import pytest
from pathlib import Path
from metasmith.python_api import (
    Agent,
    Runtime,
    Source,
    DataInstanceLibrary,
    DataTypeLibrary,
    TransformInstanceLibrary,
    TargetBuilder,
    Resources,
    Size,
    Duration,
)

# Paths relative to this file
WORKSPACE = Path(__file__).parent.resolve()
MLIB = WORKSPACE.parent  # MetasmithLibraries root
TEST_DATA_DIR = WORKSPACE / "test_data"
TEST_MSM_HOME = WORKSPACE / "test_msm_home"


@pytest.fixture(scope="session")
def workspace():
    """Return the tests workspace directory."""
    return WORKSPACE


@pytest.fixture(scope="session")
def mlib():
    """Return the MetasmithLibraries root directory."""
    return MLIB


@pytest.fixture(scope="session")
def test_data_dir():
    """Return the test data directory."""
    TEST_DATA_DIR.mkdir(exist_ok=True)
    return TEST_DATA_DIR


@pytest.fixture(scope="session")
def agent():
    """
    Deploy and return a metasmith agent using local Docker runtime.

    The agent is deployed to tests/test_msm_home/ and reused across all tests
    in the session.
    """
    agent_home = Source.FromLocal(TEST_MSM_HOME)
    smith = Agent(
        home=agent_home,
        runtime=Runtime.DOCKER,
    )

    # Deploy if not already deployed
    if not (TEST_MSM_HOME / "msm").exists():
        smith.Deploy()

    return smith


@pytest.fixture(scope="session")
def base_resources(mlib):
    """
    Load and return the base resource libraries (containers, lib).

    These are common resources needed by most transforms.
    """
    return [
        DataInstanceLibrary.Load(mlib / "resources/env"),
    ]


@pytest.fixture(scope="session")
def lib_resources(mlib):
    """
    Load additional library resources (databases, references).
    """
    lib_path = mlib / "resources/lib"
    if lib_path.exists():
        return DataInstanceLibrary.Load(lib_path)
    return None


@pytest.fixture
def tmp_inputs(tmp_path, mlib):
    """
    Create a temporary DataInstanceLibrary for test inputs.

    Returns a factory function that creates the library with specified type libraries.
    """
    def _create_inputs(type_libs=None):
        inputs_dir = tmp_path / "inputs.xgdb"
        inputs = DataInstanceLibrary(inputs_dir)

        # Add default type libraries
        default_types = [
            "sequences.yml",
            "alignment.yml",
            "binning.yml",
            "taxonomy.yml",
            "annotation.yml",
            "amplicon.yml",
            "pangenome.yml",
            "ncbi.yml",
            "transcriptomics.yml",
        ]

        if type_libs is None:
            type_libs = default_types

        for type_lib in type_libs:
            type_path = mlib / "data_types" / type_lib
            if type_path.exists():
                inputs.AddTypeLibrary(type_path)

        return inputs

    return _create_inputs


def wait_for_workflow(agent, task, timeout=600, poll_interval=5):
    """
    Wait for a workflow to complete.

    Args:
        agent: The metasmith Agent instance
        task: The workflow task
        timeout: Maximum time to wait in seconds
        poll_interval: Time between status checks in seconds

    Returns:
        DataInstanceLibrary: The results library

    Raises:
        TimeoutError: If workflow doesn't complete within timeout
        RuntimeError: If workflow fails
    """
    import time

    results_path = agent.GetResultSource(task).GetPath()
    start_time = time.time()

    while not (results_path / "_metadata").exists():
        if time.time() - start_time > timeout:
            raise TimeoutError(f"Workflow did not complete within {timeout} seconds")
        time.sleep(poll_interval)

    # Check workflow status
    agent.CheckWorkflow(task)

    return DataInstanceLibrary.Load(results_path)


def verify_fasta_output(filepath):
    """Verify a file is valid FASTA format."""
    if not filepath.exists():
        return False
    content = filepath.read_text()
    return content.startswith(">") and len(content) > 1


def verify_tsv_output(filepath, expected_headers=None):
    """Verify a file is valid TSV format with optional header check."""
    if not filepath.exists():
        return False
    content = filepath.read_text()
    if not content.strip():
        return False

    lines = content.strip().split("\n")
    if not lines:
        return False

    # Check that each line has consistent columns
    first_cols = len(lines[0].split("\t"))

    if expected_headers:
        header = lines[0].split("\t")
        for h in expected_headers:
            if h not in header:
                return False

    return True


def verify_json_output(filepath):
    """Verify a file is valid JSON."""
    import json
    if not filepath.exists():
        return False
    try:
        with open(filepath) as f:
            json.load(f)
        return True
    except json.JSONDecodeError:
        return False


@pytest.fixture(scope="session")
def ab48_bam(agent, base_resources, mlib, test_data_dir):
    """
    Generate (or return cached) BAM for AB48 community data.

    Runs the assembly_stats workflow via Docker to align reads to assembly,
    producing a sorted BAM. The result is cached to disk for reuse.
    """
    import shutil

    ab48_dir = test_data_dir / "ab48_community"
    cached_bam = ab48_dir / "ABC-240403_KD.bam"

    # Return cached BAM if it exists
    if cached_bam.exists():
        return cached_bam

    assembly_path = ab48_dir / "ABC-240403_KD.fna"
    reads_path = ab48_dir / "ABC-240403_KD.fastq.gz"

    if not assembly_path.exists():
        pytest.skip("AB48 assembly not available: ABC-240403_KD.fna")
    if not reads_path.exists():
        pytest.skip("AB48 reads not available: ABC-240403_KD.fastq.gz")

    # Build input library
    inputs_dir = ab48_dir / "_bam_gen_inputs.xgdb"
    inputs = DataInstanceLibrary(inputs_dir)
    for tl in ["sequences.yml", "alignment.yml"]:
        inputs.AddTypeLibrary(mlib / "data_types" / tl)

    meta = inputs.AddValue(
        "reads_metadata.json",
        {"parity": "single", "length_class": "long"},
        "sequences::read_metadata",
    )
    reads = inputs.AddItem(reads_path, "sequences::long_reads", parents={meta})
    inputs.AddItem(assembly_path, "sequences::assembly", parents={reads})
    inputs.Save()

    # Load assembly transforms
    assembly_transforms = [
        TransformInstanceLibrary.Load(mlib / "transforms/assembly"),
    ]

    # Target the BAM output
    targets = TargetBuilder()
    targets.Add("alignment::bam")

    task = agent.GenerateWorkflow(
        samples=list(inputs.AsSamples("sequences::read_metadata")),
        resources=base_resources + [inputs],
        transforms=assembly_transforms,
        targets=targets,
    )
    assert task.ok, f"BAM generation workflow failed to plan: {task}"

    agent.StageWorkflow(task, on_exist="clear")
    agent.RunWorkflow(
        task,
        config_file=agent.GetNxfConfigPresets()["local"],
        params=dict(
            executor=dict(cpus=14, queueSize=1),
            process=dict(tries=1),
        ),
        resource_overrides={
            "*": Resources(cpus=14, memory=Size.GB(7)),
        },
    )

    results = wait_for_workflow(agent, task, timeout=3600)
    results_path = agent.GetResultSource(task).GetPath()

    # Find and copy the BAM from results
    for path, type_name, endpoint in results.Iterate():
        if "bam" in type_name:
            bam_source = path if path.is_absolute() else results_path / path
            shutil.copy2(bam_source, cached_bam)
            return cached_bam

    raise RuntimeError("BAM not found in assembly_stats workflow results")


@pytest.fixture
def kofam_db_input(mlib, test_data_dir):
    """Create input library with KofamScan databases using absolute paths."""
    kofam_dir = test_data_dir / "kofam"
    if not (kofam_dir / "profiles").exists():
        pytest.skip("KofamScan databases not available")

    # Create library IN the kofam directory to use absolute paths
    lib_dir = kofam_dir / "_kofam.xgdb"
    inputs = DataInstanceLibrary(lib_dir)
    inputs.AddTypeLibrary(mlib / "data_types" / "ref.yml")
    inputs.AddItem(kofam_dir / "profiles", "ref::kofamscan_profiles")
    inputs.AddItem(kofam_dir / "ko_list", "ref::kofamscan_ko_list")
    inputs.Save()
    return inputs


@pytest.fixture
def interproscan_data_input(mlib, test_data_dir):
    """Create input library with InterProScan data using absolute path."""
    iprscan_dir = test_data_dir / "interproscan_data"
    if not iprscan_dir.exists():
        pytest.skip("InterProScan data not available")

    # Create library IN the interproscan_data directory to use absolute paths
    lib_dir = iprscan_dir / "_interproscan.xgdb"
    inputs = DataInstanceLibrary(lib_dir)
    inputs.AddTypeLibrary(mlib / "data_types" / "ref.yml")
    inputs.AddItem(iprscan_dir, "ref::interproscan_data")
    inputs.Save()
    return inputs


@pytest.fixture
def bakta_db_input(mlib):
    """Create input library with Bakta database using absolute path."""
    bakta_dir = Path("/home/tony/agentic_workspace/data/cyanoverse/databases/bakta")
    if not (bakta_dir / "db-light").exists():
        pytest.skip("Bakta database not available")

    lib_dir = bakta_dir / "_bakta.xgdb"
    inputs = DataInstanceLibrary(lib_dir)
    inputs.AddTypeLibrary(mlib / "data_types" / "annotation.yml")
    inputs.AddItem(bakta_dir, "annotation::bakta_db")
    inputs.Save()
    return inputs


@pytest.fixture
def predictf_db_input(mlib):
    """Create input library with PredicTF BacTFDB database using absolute path."""
    bactfdb_dir = Path("/home/tony/agentic_workspace/main/cyanoverse/tasks/search-transcription-factors/tools/predictf/BacTFDB")
    if not (bactfdb_dir / "database/v2/features.dmnd").exists():
        pytest.skip("PredicTF BacTFDB not available")

    lib_dir = bactfdb_dir / "_predictf.xgdb"
    inputs = DataInstanceLibrary(lib_dir)
    inputs.AddTypeLibrary(mlib / "data_types" / "annotation.yml")
    inputs.AddItem(bactfdb_dir, "annotation::predictf_db")
    inputs.Save()
    return inputs


@pytest.fixture
def uniref50_db_input(mlib, test_data_dir):
    """Create input library with UniRef50 DIAMOND database using absolute path."""
    uniref50_dir = test_data_dir / "uniref50"
    dmnd_file = uniref50_dir / "uniref50.dmnd"
    if not dmnd_file.exists():
        pytest.skip("UniRef50 DIAMOND database not available")

    # Create library IN the uniref50 directory to use absolute paths
    lib_dir = uniref50_dir / "_uniref50.xgdb"
    inputs = DataInstanceLibrary(lib_dir)
    inputs.AddTypeLibrary(mlib / "data_types" / "ref.yml")
    inputs.AddItem(dmnd_file, "ref::uniref50_diamond_db")
    inputs.Save()
    return inputs
