# Stand-in for `metasmith.caching.invocation`: reads the probe spec on stdin
# and answers one `key|verdict|shard` row per member. A member whose sample is
# named on the command line is a hit served from <shard_root>/<sample>.
#   python3 cache_helper.py <shard_root> [hit sample ...]
import json
import sys

spec = json.load(sys.stdin)
root, hits = sys.argv[1], set(sys.argv[2:])
for member in spec["members"]:
    sample = "+".join(sorted(member.get("reads", ["x"])))
    if sample in hits:
        print(f"key-{sample}|hit|{root}/{sample}")
    else:
        print(f"key-{sample}|miss|")
