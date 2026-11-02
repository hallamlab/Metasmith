#!/usr/bin/env python3
"""Stop or check a benchmark run by its plan key.

`cancel` is the only sanctioned stop: it removes PID.lock, and nextflow's shutdown hook
cancels the grid jobs and keeps what finished. `scancel` on the driver job skips that hook.
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _common as c  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("action", choices=["cancel", "check"])
    ap.add_argument("corpus", choices=sorted(c.HOMES))
    ap.add_argument("key")
    args = ap.parse_args()
    smith = c.get_agent(args.corpus)
    if args.action == "cancel":
        print(json.dumps(smith.CancelWorkflow(args.key), indent=2))
    else:
        smith.CheckWorkflow(args.key)
    return 0


if __name__ == "__main__":
    sys.exit(main())
