CLI Reference
############################################################

The ``metasmith`` CLI groups commands into nested subcommand verbs.
Run ``metasmith COMMAND --help`` (or ``metasmith COMMAND ACTION
--help``) for full flag listings. Every command accepts ``--json``
for machine-readable output; progress logs are routed to stderr in
that mode so stdout stays clean.

A. Global
============================================================

================================  ============================================================
Flag                              Purpose
================================  ============================================================
``--json``                        Emit machine-readable JSON on stdout
``--workspace PATH``              Workspace dir for cached tasks (env: ``METASMITH_WORKSPACE``)
``--quiet``                       Suppress non-essential output
``-V`` / ``--version``            Print version and exit
================================  ============================================================

B. Types
============================================================

==================================================  ============================================================
Command                                             Purpose
==================================================  ============================================================
``metasmith type list``                             All types, optionally ``--namespace NS``
``metasmith type show NS::NAME``                    Full details for one type
``metasmith type compat SRC TGT``                   Structural subtyping check
``metasmith type create PATH ...``                  Write a new types YAML
``metasmith type add LIB NAME --properties JSON``   Append a type to an existing YAML (``--extends``)
==================================================  ============================================================

C. Data instance libraries
============================================================

================================================================  ============================================================
Command                                                           Purpose
================================================================  ============================================================
``metasmith data inspect LIB``                                    Schema + namespaces + item count
``metasmith data list LIB [--type TYPE]``                         List items, optionally filtered
``metasmith data create PATH --type-lib ... [--purge]``           Create a new ``.xgdb`` and attach types
``metasmith data attach-types LIB TYPE_LIB --namespace NS``       Add a types YAML to an existing library
``metasmith data add-item LIB --path P --dtype T [--parent ...]`` Register an existing file
``metasmith data add-value LIB --name N --value V --dtype T``     Register a scalar/dict as a typed item
``metasmith data set-parents LIB ITEM PARENT ...``                Attach parents
``metasmith data remove LIB ITEM``                                Remove from manifest (filesystem unchanged)
``metasmith data rename LIB OLD NEW``                             Rename item (manifest + filesystem)
``metasmith data rename-by-parent LIB PARENT_TYPE``               Rename items by ancestor stem
``metasmith data prune-types LIB [--whitelist ...]``              Drop unreferenced type defs
``metasmith data consolidate LIB``                                Replace absolute-path items with local symlinks
``metasmith data save LIB``                                       Explicit ``.Save()``
``metasmith data trace LIB FROM_TYPE TO_TYPE``                    Yield ancestor/descendant pairs by type
``metasmith data load-remote URI DEST [--on-exist ...]``          Pull an ``.xgdb`` image via Logistics
``metasmith data lineage LIB ITEM``                               Show an item's type + parents
================================================================  ============================================================

D. Transform libraries
============================================================

================================================================  ============================================================
Command                                                           Purpose
================================================================  ============================================================
``metasmith transform list [--library LIB]``                      List transforms across libraries
``metasmith transform libraries``                                 Summarize transform libraries
``metasmith transform show LIB PATH``                             Inputs, outputs, group_by, resources
``metasmith transform read LIB PATH``                             Dump the ``.py`` source
``metasmith transform write LIB PATH --source FILE``              Write/overwrite a transform ``.py``
``metasmith transform scaffold LIB NAME --in T... --out T...``    Emit a typed skeleton
``metasmith transform validate LIB PATH``                         Reload + validate the contract resolves
``metasmith transform propagate-types LIB --type-lib DIR ...``    Copy type libs into the transform lib
================================================================  ============================================================

E. Workflow planning
============================================================

================================================================  ============================================================
Command                                                           Purpose
================================================================  ============================================================
``metasmith plan --data-library LIB --sample-type T \``           Plan and cache; returns ``task_key`` and ``hints``
``  --target-type T ... --transform-library L ...``
``metasmith task list``                                           List cached tasks in workspace
``metasmith task show KEY``                                       Plan + targets for one task
``metasmith task hints KEY``                                      ``PlanHint`` records for diagnosis
``metasmith task dag KEY [--format svg|text|dot]``                Render the DAG, returns the file path
``metasmith task delete KEY``                                     Remove a cached task
================================================================  ============================================================

F. Agents
============================================================

================================================================  ============================================================
Command                                                           Purpose
================================================================  ============================================================
``metasmith agent list``                                          Summarize a set of agent YAMLs
``metasmith agent info NAME``                                     Full state + config presets
``metasmith agent save PATH --home URI [--container ...]``        Write an agent YAML
``metasmith agent ping NAME [--timeout S]``                       Cheap reachability probe
``metasmith agent deploy NAME [--assertive]``                     One-time deploy (uses SSH if remote)
================================================================  ============================================================

G. Lifecycle / execution
============================================================

================================================================  ============================================================
Command                                                           Purpose
================================================================  ============================================================
``metasmith workflow stage AGENT TASK [--on-exist ...]``          Compile DAG → Nextflow and transfer
``metasmith workflow run AGENT TASK [--preset P] [--params JSON]`` Detached launch
``metasmith workflow wait AGENT TASK [--timeout S]``              Block on sentinel
``metasmith workflow tail AGENT TASK [--source agent|main] [--lines N]`` Last N lines of agent.log or main.log
``metasmith workflow cancel AGENT TASK``                          Remove ``workspace/PID.lock``; pkill fallback
``metasmith workflow runs AGENT TASK``                            All ``logs.<ts>`` directories
``metasmith workflow check TASK [--run N]``                       Same-machine status + logs
``metasmith workflow result-source AGENT TASK``                   Where results live (Globus if available)
``metasmith workflow collect AGENT TASK --dest URI``              Transfer results to a URI
``metasmith workflow presets AGENT``                              Bundled Nextflow configs
================================================================  ============================================================

H. Source / Logistics
============================================================

================================================================  ============================================================
Command                                                           Purpose
================================================================  ============================================================
``metasmith source parse URI``                                    Parse ``ssh://``, ``http(s)://``, ``globus://``, or local
``metasmith source exists URI [--timeout S]``                     Probe reachability
``metasmith source transfer SRC DEST [--no-wait]``                Logistics-backed copy
================================================================  ============================================================

I. Build
============================================================

================================================================  ============================================================
Command                                                           Purpose
================================================================  ============================================================
``metasmith build -t TYPES... -r TRANSFORMS... [-u UNIQUES...]``  Default: run types → uniques → transforms
``metasmith build types -t TYPES...``                             Just load + report type libraries
``metasmith build uniques -t TYPES... -u DIR ...``                Compile only unique resource libraries
``metasmith build transforms -t TYPES... -r DIR ...``             Compile only transform libraries
================================================================  ============================================================
