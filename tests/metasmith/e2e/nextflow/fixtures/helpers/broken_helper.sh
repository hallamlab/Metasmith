#!/bin/bash
# A cache probe that cannot answer. Every member must then run for real.
echo "mock cache helper: deliberately broken" >&2
exit 3
