Tool Reference
############################################################

The MCP server registers **62 tools and 12 resources**. They are
organised below by capability. Argument types follow Python type
hints; JSON args use the same names.

A. Server lifecycle / introspection
============================================================

================================  ============================================================
Tool                              Purpose
================================  ============================================================
``server_status()``               Loaded paths, workspace, version, cached counts
``register_type_library(path)``   Register a types YAML at runtime
``register_data_library(path)``   Register a ``.xgdb`` at runtime
``register_transform_library(p)`` Register a transform library at runtime
``register_agent(path)``          Register an agent YAML at runtime
``reload_libraries(kinds=None)``  Drop caches for any of ``types``, ``data``, ``transforms``, ``agents``
================================  ============================================================

B. Types
============================================================

==========================================  ============================================================
Tool                                        Purpose
==========================================  ============================================================
``list_types(namespace=None)``              All types, optionally filtered
``get_type(type_name)``                     Full details for ``ns::name``
``check_type_compatibility(src, tgt)``      Structural subtyping check
``create_type_library(path, ontology, types)`` Write a new types YAML
``add_type(library_path, name, properties, extends=None, overwrite=False)`` Append a type to an existing YAML
==========================================  ============================================================

C. Data instance libraries
============================================================

============================================================  ============================================================
Tool                                                          Purpose
============================================================  ============================================================
``list_data_libraries()``                                     Loaded libraries
``inspect_data_library(library_path)``                        Schema + items + namespaces
``list_data_items(library_path, type_filter=None)``           Items, optionally filtered by type
``show_item_lineage(library_path, item_path)``                Parents and properties for one item
``create_data_library(path, type_library_paths, purge=False)`` Create a new ``.xgdb`` and attach types
``attach_type_library(library, type_lib, namespace, on_exist)`` Add a types YAML to an existing library
``add_data_item(library, host_path, dtype, parents, save)``   Register an existing file
``add_data_value(library, name, value, dtype, parents, save)`` Register a scalar/dict as a typed item
``set_item_parents(library, item_path, parent_paths, save)``  Attach parents
``remove_data_item(library, item_path, save)``                Remove from manifest
``rename_data_item(library, item_path, new_path)``            Rename in manifest and on disk
``rename_by_parent(library, parent_type)``                    Rename by ancestor stem
``prune_types(library, whitelist, save)``                     Drop unreferenced type defs
``consolidate_library(library)``                              Replace absolute-path items with local symlinks
``save_library(library, update_types=True)``                  Explicit ``.Save()``
``trace_lineage(library, from_type, to_type)``                Map ancestor/descendant pairs
``load_remote_library(src_uri, dest_path, on_exist, as_image)`` Pull an ``.xgdb`` image via Logistics
============================================================  ============================================================

D. Transform libraries
============================================================

================================================================  ============================================================
Tool                                                              Purpose
================================================================  ============================================================
``list_transform_libraries()``                                    Loaded transform libraries
``list_transforms(library_path=None)``                            Transforms in a library
``show_transform_contract(library_path, transform_path)``         Inputs, outputs, group_by, resources
``read_transform_source(library_path, transform_path)``           Read ``.py``
``write_transform(library, transform_path, source, register)``    Write or overwrite a transform ``.py``
``scaffold_transform(library, name, inputs, outputs, group_by, container_type, resources)`` Emit a typed skeleton
``validate_transform_contract(library, transform_path)``          Reload + introspect contract (no container pull)
``propagate_types(transform_library)``                            Copy registered type libs into the transform lib
================================================================  ============================================================

E. Workflow planning
============================================================

==========================================  ============================================================
Tool                                        Purpose
==========================================  ============================================================
``plan_workflow(data_library, sample_type, target_types, transform_libraries, resource_libraries=None)`` Plan and cache; returns ``task_key`` and ``hints``
``get_workflow_plan(task_key)``             Re-fetch a saved plan
``get_plan_hints(task_key)``                ``PlanHint`` records for diagnosis
``render_plan_dag(task_key, format, blacklist_namespaces)`` Render the DAG, returns the file path
``list_workflow_tasks()``                   Workspace inventory
``delete_workflow_task(task_key)``          Remove cached task
==========================================  ============================================================

F. Agents
============================================================

==========================================  ============================================================
Tool                                        Purpose
==========================================  ============================================================
``list_agents()``                           Loaded agents
``load_agent(agent_path, name=None)``       Load an agent YAML
``save_agent(path, home_uri, container, runtime, setup_commands, globus_uuid)`` Write an agent YAML
``get_agent_info(agent_name)``              Full state + config presets
``agent_ping(agent_name, timeout_s=15)``    Cheap reachability probe
``deploy_agent(agent_name, assertive=False)`` One-time deploy (uses SSH if remote)
==========================================  ============================================================

G. Lifecycle / execution
============================================================

==========================================  ============================================================
Tool                                        Purpose
==========================================  ============================================================
``stage_workflow(agent_name, task_key, on_exist)`` Compile DAG → Nextflow and transfer
``run_workflow(agent_name, task_key, config_preset, params, resource_overrides, stub_delay)`` Detached launch
``wait_for_workflow(agent_name, task_key, timeout_s, poll_s, run, since_mtime)`` Block on sentinel
``tail_workflow_log(agent_name, task_key, source, lines, run)`` Last N lines of agent.log or main.log
``cancel_workflow(agent_name, task_key, timeout_s=30)`` Remove ``workspace/PID.lock``; pkill fallback
``list_workflow_runs(agent_name, task_key)`` All ``logs.<ts>`` directories
``check_workflow(task_key, run=None)``      Same-machine status + logs
``get_result_source(agent_name, task_key)`` Where results live (Globus if available)
``collect_results(agent_name, task_key, dest_uri, allow_globus=True)`` Transfer results to a URI
``list_config_presets(agent_name)``         Bundled Nextflow configs
==========================================  ============================================================

H. Source / Logistics
============================================================

==========================================  ============================================================
Tool                                        Purpose
==========================================  ============================================================
``parse_source(uri)``                       Parse ``ssh://``, ``http(s)://``, ``globus://``, or a local path
``source_exists(uri, timeout_s=10)``        Probe reachability
``transfer_source(src_uri, dest_uri, wait=True, label=None)`` Logistics-backed copy
==========================================  ============================================================

I. Build
============================================================

================================================  ============================================================
Tool                                              Purpose
================================================  ============================================================
``build_libraries(type_paths, transform_paths)``  Compile types and propagate to transform libs
================================================  ============================================================

J. Resources (URI views)
============================================================

============================================  ============================================================
URI                                           Returns
============================================  ============================================================
``metasmith://server/status``                 ``server_status``
``metasmith://types``                         Namespaces → counts
``metasmith://types/{ns}``                    Types in a namespace
``metasmith://types/{ns}/{name}``             One type
``metasmith://data/{lib}``                    Items in a library
``metasmith://transforms/{lib}``              Transforms in a library
``metasmith://transforms/{lib}/{name}``       Contract details
``metasmith://agents``                        Loaded agents
``metasmith://agents/{name}``                 One agent
``metasmith://tasks``                         Cached tasks
``metasmith://tasks/{key}``                   Plan + targets
``metasmith://tasks/{key}/dag``               Rendered DAG path
============================================  ============================================================
