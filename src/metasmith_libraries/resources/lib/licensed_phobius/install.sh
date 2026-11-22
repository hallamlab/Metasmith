#!/bin/sh
# UNVERIFIED against a real download. Runs as root inside the build, from the
# Phobius standalone archive (phobius101_linux.tar.gz): phobius.pl beside its
# decodeanhmm binary and model files, run in place from /opt/phobius.
set -eu
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends perl
rm -rf /var/lib/apt/lists/*
mkdir -p /opt/phobius /tmp/phobius
tar -xf "$1" -C /tmp/phobius
src=$(dirname "$(find /tmp/phobius -name phobius.pl | head -1)")
cp -r "$src"/. /opt/phobius/
rm -rf /tmp/phobius
chmod -R a+rX /opt/phobius
ln -sf /opt/phobius/phobius.pl /usr/local/bin/phobius
