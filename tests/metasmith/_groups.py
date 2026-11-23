"""Feature groups and the smoke set, for picking what a merge check runs.

A test file's group is its axis directory unless OVERRIDES names the file. This module
imports nothing from metasmith, so dev/metasmith.sh reads it without collecting the suite:

    python tests/metasmith/_groups.py paths cache flow   # cheap-tier paths of those groups
    python tests/metasmith/_groups.py smoke              # the smoke node ids
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent

# The directories a merge check may run: no container, no host Nextflow, no wall-clock budget.
CHEAP_DIRS = ["unit", "flow", "solver", "cache", "gui", "bootstrap", "audit", "e2e/virtual"]

DIR_GROUPS = {
    "solver": "solver",
    "flow": "flow",
    "cache": "cache",
    "gui": "gui",
    "bootstrap": "bootstrap",
    "audit": "audit",
    "perf": "perf",
    "deploy": "host",
    "lifecycle": "run",
    "e2e/docker": "docker",
    "e2e/nextflow": "flow",
    "e2e/agentic": "agentic",
}


def _files(dir_: str, *names: str) -> list[str]:
    return [f"{dir_}/test_{n}.py" for n in names]


# unit/ and e2e/virtual/ mix features, so each of their files is named here.
OVERRIDES: dict[str, list[str]] = {
    "host": _files(
        "unit", "agent_yml_legacy", "deploy_probe", "dev_overlay_deploy", "live_shell",
        "local_transfer", "parse_path_apptainer_relative", "parse_path_symlinks", "paths",
        "replace_path_overlap", "sbatch_array_phantom_guard", "sbatch_dir_named_ws",
        "sbatch_home_root_cwd", "source_parse", "str_replace_path_overlap", "watcher_owner",
    ),
    "flow": _files(
        "unit", "api_exit_codes", "bind_path_validation", "emitted_address_contract",
        "grouping_contract", "lineage_schema",
    ) + _files("e2e/virtual", "contract_smoke", "stage_two_task_keys", "virtual_pipeline", "telemetry_e2e"),
    "library": _files(
        "unit", "binner_oom_exit", "build_decomposition", "database_downloads", "deferred_paths",
        "fetch_and_memory", "fork_roundtrip", "library_view_ordering", "pinned_library",
        "product_groups", "prune_types", "transform_load_is_serialised",
    ),
    "pkg": _files(
        "unit", "container_tag", "dev_sh_dist_guard", "dev_sh_tag", "docker_build_context",
        "marker_taxonomy", "module_surface", "no_import_cycles", "package_data_covers_shipped_files",
        "renamed_symbols_are_gone", "repo_root_anchors", "solver_engine_packaging",
        "vendor_library_metadata",
    ),
    "api": _files("unit", "cli", "input_rows", "ops", "sample_tables", "tutorial_replay"),
    "bootstrap": _files("unit", "context_provenance", "setup_environment", "step_exit_codes")
    + _files("e2e/virtual", "gpu_staging", "rootfs_staging", "virtual_multicontainer"),
    "dag": _files(
        "unit", "dag_blacklist_defaults", "dag_colour", "dag_geometry_wire", "dag_layout",
        "dag_modes", "dag_rails", "dag_renderer", "dag_stress",
    ),
    "run": _files(
        "unit", "failed_steps", "launch_headroom", "nxf_task_metadata", "resource_ceiling_preflight",
        "retry_ladder", "run_heartbeat", "run_host_hop", "run_workflow_gpu_preflight",
        "slurm_account_guard", "stage_engine_check", "unlimited_duration", "unread_params",
        "wait_for_workflow",
    ) + _files("e2e/virtual", "ignored_step_is_surfaced"),
    "cache": _files("unit", "lineage_report", "log_capture", "remote_pool_read")
    + _files("e2e/virtual", "cache_baseline", "cache_e2e"),
    "agentic": _files("unit", "agentic_runs_dir_is_ignored") + _files("deploy", "sandbox_unit"),
}

_FILE_GROUP = {f: g for g, files in OVERRIDES.items() for f in files}

# One to three cheap tests per group, plus one run through every layer.
SMOKE = [
    "e2e/virtual/test_cache_e2e.py::test_full_caching_flow_end_to_end",
    "solver/test_solver.py::TestBasicSolver::test_simple_chain",
    "solver/test_engine_solve.py::test_a_problem_with_no_route_is_refused_rather_than_answered",
    "solver/test_rng_contract.py::TestChaChaCore::test_rfc8439_block_vector",
    "flow/test_spec.py::test_both_doors_reach_the_same_plan",
    "flow/test_plan_hints.py::test_unreachable_target",
    "flow/test_forks.py::test_each_outcome_of_a_fork_gets_its_own_route",
    "flow/test_linear.py::test_l1_single_step_round_trip",
    "e2e/virtual/test_contract_smoke.py::test_contract_runtime_two_step_linear_identity",
    "cache/test_wire_version_sync.py::test_groovy_reserved_keys_match_the_parser",
    "bootstrap/test_libraries.py::TestDataInstanceLibrarySaveLoad::test_save_load_preserves_grandparent_lineage",
    "unit/test_pinned_library.py::test_a_pinned_library_refuses_every_mutation",
    "unit/test_prune_types.py::test_pruning_types_does_not_move_the_library_key",
    "cache/test_member_key.py::test_two_members_with_the_same_consumed_ids_share_a_key",
    "cache/test_promote.py::test_a_promotion_leaves_no_staging_behind",
    "cache/test_incomplete_shard.py::test_a_shard_missing_a_listed_file_is_not_a_hit",
    "bootstrap/test_container_golden.py::TestRunCommandGolden::test_docker",
    "bootstrap/test_exec_with_env_contract.py::test_container_runtime_issues_a_run_command",
    "bootstrap/test_image_preflight.py::TestWhichImages::test_distinct_images_across_steps",
    "unit/test_paths.py::TestPathMapConversionMatrix::test_local_home_root_to_external",
    "unit/test_live_shell.py::test_g1_exit_code_nonzero",
    "unit/test_source_parse.py::TestSshRoundTrip::test_colon_form_parses",
    "unit/test_wait_for_workflow.py::test_a_live_driver_after_nextflow_exits_is_not_an_error",
    "unit/test_retry_ladder.py::TestLadder::test_memory_doubles_only_on_an_oom",
    "bootstrap/test_run_lifecycle_contract.py::TestDriverStopsNextflowByGroup::test_the_lock_file_is_the_cancel_handle",
    "unit/test_ops.py::TestWorkflowOps::test_plan_success_persists_task",
    "unit/test_cli.py::TestCLISmoke::test_help_lists_subcommands",
    "unit/test_dag_renderer.py::test_to_dot_is_insertion_ordered_and_deterministic",
    "unit/test_dag_layout.py::test_a_small_graph_is_proven_shortest",
    "unit/test_dag_rails.py::test_a_named_shape_is_unambiguous",
    "gui/test_gui_backend.py::TestProject::test_types_are_listed",
    "gui/test_gui_backend.py::TestWorkflowGenerateMore::test_a_result_echoes_what_it_planned_from",
    "gui/test_gui_cli.py::TestCommandSurface::test_gui_is_registered",
    "unit/test_marker_taxonomy.py::test_every_axis_directory_is_mapped",
    "unit/test_no_import_cycles.py::test_no_unexpected_import_cycles",
]


def group_of(rel: str) -> str | None:
    if rel in _FILE_GROUP:
        return _FILE_GROUP[rel]
    for dir_, group in sorted(DIR_GROUPS.items(), key=lambda kv: -len(kv[0])):
        if rel.startswith(dir_ + "/"):
            return group
    return None


def groups() -> set[str]:
    return set(DIR_GROUPS.values()) | set(OVERRIDES)


def cheap_paths(names: list[str]) -> list[str]:
    unknown = set(names) - groups()
    if unknown:
        raise SystemExit(f"unknown group(s) {sorted(unknown)}; known: {sorted(groups())}")
    out = []
    for dir_ in CHEAP_DIRS:
        if DIR_GROUPS.get(dir_) in names:
            out.append(dir_)
            continue
        out += sorted(
            p.relative_to(ROOT).as_posix()
            for p in (ROOT / dir_).rglob("test_*.py")
            if group_of(p.relative_to(ROOT).as_posix()) in names
        )
    return out


if __name__ == "__main__":
    verb, *args = sys.argv[1:] or ["help"]
    if verb == "paths":
        print("\n".join(f"tests/metasmith/{p}" for p in cheap_paths(args)))
    elif verb == "smoke":
        print("\n".join(f"tests/metasmith/{n}" for n in SMOKE))
    elif verb == "list":
        print(" ".join(sorted(groups())))
    else:
        raise SystemExit(__doc__)
