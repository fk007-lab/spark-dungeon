#!/bin/sh
# Auto-detect JAVA_HOME (path differs between arm64 and amd64 on Debian)
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
export PATH="$JAVA_HOME/bin:$PATH"
exec "$@"
