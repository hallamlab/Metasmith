#!/bin/sh
# Runs as root inside the build, from the SRI installer the user downloaded
# (pathway-tools-27.0-linux-64-tier1-install, an InstallBuilder binary). Its
# unattended mode lays Pathway Tools out where lib::pathologic/run.sh looks:
# the launcher at /opt/pathway-tools/pathway-tools and ptools-local at
# /data/ptools-local. The packages are what the launcher's Lisp image links.
# It dlopens an unversioned libcrypto.so, which only libssl-dev provides; without
# it the installer's own first launch fails silently and writes no
# ptools-init.dat, which run.sh copies.
set -eu
export DEBIAN_FRONTEND=noninteractive
# A throwaway build root needs no fsync per package, and on a busy disk it stalls.
echo force-unsafe-io > /etc/dpkg/dpkg.cfg.d/msm-unsafe-io
apt-get update
apt-get install -y --no-install-recommends xterm openssl libxml2 libxm4 libssl-dev procps ca-certificates
rm -rf /var/lib/apt/lists/*
cp "$1" /tmp/pt-install
chmod +x /tmp/pt-install
/tmp/pt-install --mode unattended --InstallDir /opt/pathway-tools --PTOOLS_LOCAL_PATH /data --InstallDesktopShortcuts 0
rm -f /tmp/pt-install
chmod -R a+rX /opt/pathway-tools /data
test -x /opt/pathway-tools/pathway-tools
test -f /data/ptools-local/ptools-init.dat
