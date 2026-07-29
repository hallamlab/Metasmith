# tests/gui

The web GUI's own suite, and the inner loop while working on the page:
`./dev.sh -tg` runs exactly this directory.

Default markers: `fast` + `gui`. The directory is what grants them — the
`pytestmark = pytest.mark.gui` lines still in these files are local reminders of
what they are, not what selects them.

| File | Surface |
|---|---|
| `test_gui_backend.py` | The Flask API: routes, project state, the job runner. |
| `test_gui_cli.py` | `msm gui` — the entry point and its arguments. |
| `test_gui_ssh.py` | Reading and editing the ssh config the page offers. |
| `test_fork_identity.py` | The fork discriminator the page exposes. Its *cache* consequence is pinned separately in `tests/cache/test_fork_busts_cache.py` — a fork whose ids survive replays the run it was made to escape. |
| `test_dev_sh_gui_guard.py` | `dev.sh` refusing to package without a built bundle. |

Keep it fast. A GUI test that needs a docker daemon is an e2e test and belongs
in `tests/e2e/docker/`.
