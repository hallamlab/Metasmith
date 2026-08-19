import pytest
from pathlib import Path

collect_ignore = ["manual"]
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

WORKSPACE = Path(__file__).parent.resolve()
MLIB = WORKSPACE.parent.parent / "src" / "metasmith_libraries"
assert (MLIB / "data_types").is_dir(), f"library root does not look like one: {MLIB}"
TEST_DATA_DIR = WORKSPACE / "test_data"
TEST_MSM_HOME = WORKSPACE / "test_msm_home"


@pytest.fixture(scope="session")
def workspace():
    return WORKSPACE


@pytest.fixture(scope="session")
def mlib():
    return MLIB


@pytest.fixture(scope="session")
def test_data_dir():
    TEST_DATA_DIR.mkdir(exist_ok=True)
    return TEST_DATA_DIR


def _discard_stale_derived_home():
    # runs/ and task_cache/ are keyed on the file-identity and cache-key contracts. A shard
    # written under one contract must never be replayed under another, so both are dropped
    # whenever the stamp disagrees with the versions this tree compiles against.
    import shutil
    from metasmith.caching.keys import CACHE_KEY_VERSION, LIN_PAYLOAD_VERSION

    stamp_file = TEST_MSM_HOME / ".contract_stamp"
    stamp = f"cache_key={CACHE_KEY_VERSION} lin_payload={LIN_PAYLOAD_VERSION}\n"
    if stamp_file.is_file() and stamp_file.read_text() == stamp:
        return

    for name in ("runs", "task_cache"):
        target = TEST_MSM_HOME / name
        assert target.parent == TEST_MSM_HOME and target.name == name
        if target.is_dir():
            shutil.rmtree(target)

    TEST_MSM_HOME.mkdir(parents=True, exist_ok=True)
    stamp_file.write_text(stamp)


@pytest.fixture(scope="session")
def agent():
    _discard_stale_derived_home()

    agent_home = Source.FromLocal(TEST_MSM_HOME)
    smith = Agent(
        home=agent_home,
        runtime=Runtime.DOCKER,
    )

    if not (TEST_MSM_HOME / "msm").exists():
        smith.Deploy()

    return smith


@pytest.fixture(scope="session")
def base_resources(mlib):
    return [
        DataInstanceLibrary.Load(mlib / "resources/env"),
    ]


@pytest.fixture(scope="session")
def lib_resources(mlib):
    lib_path = mlib / "resources/lib"
    if lib_path.exists():
        return DataInstanceLibrary.Load(lib_path)
    return None


@pytest.fixture
def tmp_inputs(tmp_path, mlib):
    def _create_inputs(type_libs=None):
        inputs_dir = tmp_path / "inputs.xgdb"
        inputs = DataInstanceLibrary(inputs_dir)

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
    import time

    results_path = agent.GetResultSource(task).GetPath()
    start_time = time.time()

    while not (results_path / "_metadata").exists():
        if time.time() - start_time > timeout:
            raise TimeoutError(f"Workflow did not complete within {timeout} seconds")
        time.sleep(poll_interval)

    agent.CheckWorkflow(task)

    return DataInstanceLibrary.Load(results_path)


def verify_fasta_output(filepath):
    if not filepath.exists():
        return False
    content = filepath.read_text()
    return content.startswith(">") and len(content) > 1


def verify_tsv_output(filepath, expected_headers=None):
    if not filepath.exists():
        return False
    content = filepath.read_text()
    if not content.strip():
        return False

    lines = content.strip().split("\n")
    if not lines:
        return False

    first_cols = len(lines[0].split("\t"))

    if expected_headers:
        header = lines[0].split("\t")
        for h in expected_headers:
            if h not in header:
                return False

    return True


def verify_json_output(filepath):
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
    import shutil

    ab48_dir = test_data_dir / "ab48_community"
    cached_bam = ab48_dir / "ABC-240403_KD.bam"

    if cached_bam.exists():
        return cached_bam

    assembly_path = ab48_dir / "ABC-240403_KD.fna"
    reads_path = ab48_dir / "ABC-240403_KD.fastq.gz"

    if not assembly_path.exists():
        pytest.skip("AB48 assembly not available: ABC-240403_KD.fna")
    if not reads_path.exists():
        pytest.skip("AB48 reads not available: ABC-240403_KD.fastq.gz")

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

    assembly_transforms = [
        TransformInstanceLibrary.Load(mlib / "transforms/assembly"),
    ]

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

    for path, type_name, endpoint in results.Iterate():
        if "bam" in type_name:
            bam_source = path if path.is_absolute() else results_path / path
            shutil.copy2(bam_source, cached_bam)
            return cached_bam

    raise RuntimeError("BAM not found in assembly_stats workflow results")


@pytest.fixture
def kofam_db_input(mlib, test_data_dir):
    kofam_dir = test_data_dir / "kofam"
    if not (kofam_dir / "profiles").exists():
        pytest.skip("KofamScan databases not available")

    lib_dir = kofam_dir / "_kofam.xgdb"
    inputs = DataInstanceLibrary(lib_dir)
    inputs.AddTypeLibrary(mlib / "data_types" / "ref.yml")
    inputs.AddItem(kofam_dir / "profiles", "ref::kofamscan_profiles")
    inputs.AddItem(kofam_dir / "ko_list", "ref::kofamscan_ko_list")
    inputs.Save()
    return inputs


@pytest.fixture
def interproscan_data_input(mlib, test_data_dir):
    iprscan_dir = test_data_dir / "interproscan_data"
    if not iprscan_dir.exists():
        pytest.skip("InterProScan data not available")

    lib_dir = iprscan_dir / "_interproscan.xgdb"
    inputs = DataInstanceLibrary(lib_dir)
    inputs.AddTypeLibrary(mlib / "data_types" / "ref.yml")
    inputs.AddItem(iprscan_dir, "ref::interproscan_data")
    inputs.Save()
    return inputs


@pytest.fixture
def bakta_db_input(mlib):
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
    uniref50_dir = test_data_dir / "uniref50"
    dmnd_file = uniref50_dir / "uniref50.dmnd"
    if not dmnd_file.exists():
        pytest.skip("UniRef50 DIAMOND database not available")

    lib_dir = uniref50_dir / "_uniref50.xgdb"
    inputs = DataInstanceLibrary(lib_dir)
    inputs.AddTypeLibrary(mlib / "data_types" / "ref.yml")
    inputs.AddItem(dmnd_file, "ref::uniref50_diamond_db")
    inputs.Save()
    return inputs
