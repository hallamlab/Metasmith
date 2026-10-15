#!/usr/bin/env python3
"""Author every KBase template, and fail on any that no longer solves.

Mirrors src/metasmith_libraries/build_templates.py. Run it after
regenerating the library: a transform whose products change shape takes its
templates down here, by name, rather than in someone's GUI a week later.
"""
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE / 'templates_src'))
import _authoring as A

AUTHORS = [
    "fba_model_from_genome",
    "genome_from_any_assembly",
    "fba_from_genome",
    "report_from_any_paired_end_library",
    "single_end_library_from_any_paired_end_library",
    "tree_from_any_assembly",
    "fba_model_from_genome_2",
    "single_end_library_from_any_paired_end_library_2",
    "tree_from_any_assembly_2",
    "fba_from_fba_model",
    "reads_set_from_any_paired_end_library",
    "fba_from_any_assembly"
]

BLOCKED = {}

def main():
    wanted = sys.argv[1:] or AUTHORS
    for name in wanted:
        if name in BLOCKED:
            print(f'  {name}: BLOCKED -- {BLOCKED[name]}'); continue
        A.author(__import__(name), rebuild='--rebuild' in sys.argv)

if __name__ == '__main__':
    main()
