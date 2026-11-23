#!/bin/sh
# Runs as root inside the build, from the Phobius 1.01 standalone archive
# (phobius101_linux.tgz): a phobius/ directory with phobius.pl beside its model
# files, run in place from /opt/phobius as its README says. The shipped
# `decodeanhmm` is a 32-bit binary that a 64-bit image cannot run, so the
# static decodeanhmm.64bit takes its name.
set -eu
export DEBIAN_FRONTEND=noninteractive
# A throwaway build root needs no fsync per package, and on a busy disk it stalls.
echo force-unsafe-io > /etc/dpkg/dpkg.cfg.d/msm-unsafe-io
apt-get update
apt-get install -y --no-install-recommends perl
rm -rf /var/lib/apt/lists/*
mkdir -p /opt/phobius /tmp/phobius
tar -xf "$1" -C /tmp/phobius
src=$(dirname "$(find /tmp/phobius -name phobius.pl | head -1)")
cp -r "$src"/. /opt/phobius/
rm -rf /tmp/phobius
mv /opt/phobius/decodeanhmm.64bit /opt/phobius/decodeanhmm
chmod -R a+rX /opt/phobius
chmod 755 /opt/phobius/decodeanhmm /opt/phobius/phobius.pl
ln -sf /opt/phobius/phobius.pl /usr/local/bin/phobius
phobius -short /opt/phobius/OPSD_SHEEP | grep -q "^OPSD_SHEEP *7 "
