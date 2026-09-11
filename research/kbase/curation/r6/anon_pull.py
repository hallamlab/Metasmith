"""For each env resource, ask the registry whether an ANONYMOUS pull resolves its image."""
import json, re, sys, urllib.request, urllib.error
from pathlib import Path
import yaml

ACCEPT = ",".join([
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
])

def get(url, headers=None, timeout=30):
    req = urllib.request.Request(url, headers=headers or {})
    return urllib.request.urlopen(req, timeout=timeout)

def check(uri):
    ref = uri.removeprefix("docker://")
    if "@" in ref: repo, tag = ref.split("@", 1)
    elif ":" in ref.rsplit("/", 1)[-1]: repo, tag = ref.rsplit(":", 1)
    else: repo, tag = ref, "latest"
    if repo.startswith("quay.io/"):
        host, path = "quay.io", repo[len("quay.io/"):]
        auth = f"https://quay.io/v2/auth?service=quay.io&scope=repository:{path}:pull"
    elif "/" in repo and "." in repo.split("/")[0]:
        return "?", "unknown registry"
    else:
        host, path = "registry-1.docker.io", repo if "/" in repo else f"library/{repo}"
        auth = f"https://auth.docker.io/token?service=registry.docker.io&scope=repository:{path}:pull"
    try:
        token = json.load(get(auth))["token"]
    except Exception as e:
        return "ERR", f"auth: {e}"
    try:
        r = get(f"https://{host}/v2/{path}/manifests/{tag}",
                {"Authorization": f"Bearer {token}", "Accept": ACCEPT})
        return str(r.status), ""
    except urllib.error.HTTPError as e:
        return str(e.code), ""
    except Exception as e:
        return "ERR", str(e)[:60]

D = Path("src/metasmith_libraries/resources/env")
rows = []
for p in sorted(D.glob("*.env")):
    try:
        d = yaml.safe_load(p.read_text())
    except Exception:
        d = None
    uri = d.get("container") if isinstance(d, dict) else None
    if not uri:
        rows.append((p.name, "-", "no container: line", ""))
        continue
    code, note = check(str(uri))
    rows.append((p.name, code, str(uri), note))

for name, code, uri, note in rows:
    mark = "" if code in ("200", "-") else "   <<<"
    print(f"{code:>4}  {name:<34} {uri}{(' ' + note) if note else ''}{mark}")
bad = [r for r in rows if r[1] not in ("200", "-")]
print(f"\n{len(rows)} env files, {len([r for r in rows if r[1]=='-'])} with no container:, "
      f"{len(bad)} an anonymous pull cannot resolve")
