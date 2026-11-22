#!/bin/sh
# Runs as root inside the build, from the SRI installer the user downloaded
# (pathway-tools-27.0-linux-64-tier1-install). Lays Pathway Tools out where
# lib::pathologic/run.sh looks: the launcher at /opt/pathway-tools/pathway-tools
# and ptools-local at /data/ptools-local. The answers piped to the installer are
# destination, data folder, then its prompts (MetaPathways' docker/Dockerfile_ptools).
set -eu
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends xterm openssl libxml2 xvfb libxm4 procps less ca-certificates
rm -rf /var/lib/apt/lists/*
cp "$1" /tmp/pt-install
chmod +x /tmp/pt-install
printf '/opt/pathway-tools\n/data\n\nn\nY\nn\n\n' | /tmp/pt-install
rm -f /tmp/pt-install
/opt/pathway-tools/pathway-tools -lisp -eval '(exit)' </dev/null || true
mkdir -p /data/ptools-local
[ -f /data/ptools-local/ptools-init.dat ] || printf '#\nWWW-Server-Hostname localhost\nWWW-Server-Port 1555\n' > /data/ptools-local/ptools-init.dat
chmod -R a+rX /opt/pathway-tools /data
test -x /opt/pathway-tools/pathway-tools
