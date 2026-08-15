#!/bin/bash
# Compile the build_references transform library's _metadata/ indexes.
#
# Two type directories, not one. LoadTypeLibraries keys a namespace off the YAML
# filename stem and RAISES on a duplicate, so a `ref.yml` here would collide with
# the shipped library's. The split falls out of that:
#
#   src/metasmith_libraries/data_types/   env:: lib:: ncbi:: sequences:: annotation::
#                                         ref:: fabfos::    -- shared, run-side too
#   build_references/data_types/          raw:: interm:: bench:: buildlib::
#                                                            -- build-only
#
# A new type that a RUN-side transform will consume (ref::mnxr_lookup, say) belongs
# in the shipped library, not here.
#
# The same split applies to CODE, which is why there are two uniques loops below.
# `lib::` (submodule) ships in the wheel because a fosmid run executes it; `buildlib::`
# does not, because nothing in it runs outside a reference compile. Shipping the
# reaction-universe mappers and the thermodynamics ensemble to someone who installed
# FabFos to assemble fosmids is the thing that split is there to prevent.
#
# The shipped library's own transform dirs are rebuilt too: they carry their own
# pruned copy of the type graph under transforms/*/_metadata/types/, so a change to
# ref.yml that they reference does not reach them until they are recompiled.
set -euo pipefail
HERE="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
# up three: build_references/ -> fabfos/ -> src/ -> the repository root. The
# monorepo nested the package one level deeper than the standalone fabfos repo
# this script was written in.
REPO="$(cd "$HERE/../../.." && pwd)"
MLIB="$REPO/src/metasmith_libraries"

if command -v msm >/dev/null 2>&1; then
    msm=(msm)
else
    msm=(python -m metasmith)
fi

# --- vendor ecspr into the library ------------------------------------------
# The bake method is a python package (`ecspr.bake`), not a pile of flat files,
# and a transform reaches it as `python3 -m ecspr.bake.<lane>.<module>`. So the
# library has to carry a copy: `buildlib::ecspr` is ONE staged input whose
# instance_id is the tree digest, which is what carries provenance from source
# to baked result and what lets an unchanged tree re-stage into a cache hit.
#
# Generated, gitignored, never edited in place -- the same arrangement as
# `dev/fabfos.sh --bundle-library`. It is regenerated HERE, immediately before
# the metadata compile, because a copy made by some other invocation is a copy
# the index does not describe.
#
# Copy, do not link: Logistics copies symlinks as symlinks, so a linked vendor
# would stage a dangling path on any host that is not this one. The exclusions
# are the three things that change without the code changing -- leave one in and
# the id moves for a reason nobody can review.
ECSPR_SRC="$REPO/src/ecspr"
ECSPR_DST="$HERE/resources/buildlib/ecspr"
echo "== vendoring $ECSPR_SRC -> $ECSPR_DST"
rm -rf "$ECSPR_DST"
mkdir -p "$ECSPR_DST"
tar -C "$ECSPR_SRC" -cf - \
    --exclude='__pycache__' --exclude='*.pyc' \
    --exclude='.egg-info' --exclude='*.egg-info' \
    --exclude='build_hash.txt' --exclude='build' --exclude='dist' \
    . | tar -C "$ECSPR_DST" -xf -

args=(build all
      --types "$MLIB/data_types"
      --types "$HERE/data_types")

# A leading underscore means "written, not in the library" -- see
# transforms/_deferred/README.md. Loading one would fail the build on a type that
# does not exist yet, and take every gate with it.
for d in "$MLIB"/resources/*/;   do case $(basename "$d") in _*) continue;; esac; args+=(--uniques    "${d%/}"); done
for d in "$HERE"/resources/*/;   do case $(basename "$d") in _*) continue;; esac; args+=(--uniques    "${d%/}"); done
for d in "$MLIB"/transforms/*/;  do case $(basename "$d") in _*) continue;; esac; args+=(--transforms "${d%/}"); done
for d in "$HERE"/transforms/*/;  do case $(basename "$d") in _*) continue;; esac; args+=(--transforms "${d%/}"); done

echo "== ${msm[*]} ${args[*]}"
PYTHONPATH="${PYTHONPATH:-}" "${msm[@]}" "${args[@]}"
