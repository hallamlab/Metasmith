# Where the KBase material came from

## Purpose & Contents

This file records every KBase service this port reads, the call it makes, and what that call
yields. It is the record that lets a later reader re-fetch the corpus or date it.

Per-artifact fetch dates are written by the fetchers into `catalog/census.json` and the raw cache
headers. This file records the routes, not the runs.

## Every route is public

No KBase account, token or API key is used anywhere in this port. Every call below was verified
unauthenticated on 2026-09-04.

## The services

| service | endpoint | what it gives |
|---|---|---|
| Catalog | `https://kbase.us/services/catalog` | `list_basic_module_info` lists 137 registered modules with a git URL each. `get_module_version` gives the pinned git commit and the docker image for one module. |
| Narrative Method Store | `https://kbase.us/services/narrative_method_store/rpc` | `list_methods` lists all 493 apps with their input and output workspace types. `get_method_spec` gives one app's full spec, including per-parameter `valid_ws_types` and `is_output_name`. |
| Workspace | `https://kbase.us/services/ws` | `get_type_info` gives one workspace type's description, KIDL spec and JSON schema. `get_objects2` reads a public narrative object. `list_workspace_info` lists 4,690 public workspaces. |
| Search | `https://kbase.us/services/searchapi2/rpc` | `search_objects` over the `narrative` index enumerates 3,965 public narratives with title, owner, cell summaries and object types. |

**CAUTION** `get_method_spec` needs `tag` supplied explicitly. Without it a dynamically registered
module fails with "Repository <name> wasn't registered", which reads as a missing module rather than
a missing argument. `scrape/fetch.py` tries release, then beta, then dev, then no tag, which
resolved all 493 apps.

**CAUTION** These Java services answer a bad request with HTTP 500 carrying a JSON-RPC error body,
not with a transport failure. A client that retries on the status code spends its whole backoff on a
deterministic answer. `scrape/kbase_api.py` parses the body of a 500 and raises immediately. The
message is the useful part: "Unable to locate type", "Module doesn't exist" and "could not be split
into a module and name" are three different data conditions.

## The git repositories

The apps are not in `github.com/kbase`. That org holds platform infrastructure. Each app module has
its own repository, named by the Catalog's `git_url`:

- 109 modules under `github.com/kbaseapps`
- 6 under `github.com/kbase`
- 20 under personal accounts
- 2 on GitLab

The Catalog is the only thing that knows which repository an app belongs to. Do not infer it from
the app id.

## What the census counted on 2026-09-04

493 apps, of which 235 active, 431 runnable and 62 viewers, and 478 declaring typed inputs or
outputs. 422 resolve to a catalog module. 121 distinct workspace types across 49 type modules. 137
catalog modules. 3,965 public narratives.

Of the 121 referenced types, 106 resolve. The other 15 are dangling references in KBase's own app
specs: 7 retired types, 6 whose entire type module is gone, and 2 that are not type names at all
(`*` and a bare `KBaseMatrices`). 48 apps reference at least one of them.

An app names what it produces through one of two channels, and only the first is a parameter flag.
138 active apps carry a typed output parameter. 207 declare a KBaseReport in their service output
mapping. Together 217 of the 235 active apps name some product. The remaining 18 name none: they are
in-browser visualizations, a staging exporter and an ACL update, none of which creates an object.

These are the numbers the port's percentages are quoted against. `catalog/census.json` is the
machine-readable form and supersedes this paragraph if the two disagree.
