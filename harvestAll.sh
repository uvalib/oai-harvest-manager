#!/bin/bash
# ------------------------------------------------------------
# Resolve script home directory
# ------------------------------------------------------------

HOMEDIR=$(cd -P "$(dirname "$0")" && pwd)

# ------------------------------------------------------------
# First argument = config file
# Remaining arguments are passed through to Java
# ------------------------------------------------------------

CONFIGFILE="$1"
shift

CONFIGDIR=$(cd -P "$(dirname "$CONFIGFILE")" && pwd)
CONFIG=$(basename "$CONFIGFILE")

# ------------------------------------------------------------
# Classpath
# ------------------------------------------------------------

CLASSPATH="$HOMEDIR/target/oai-harvest-manager-1.2.1.jar:$HOMEDIR/target/lib/*"

JAVAARGS=""

MAINCLASS="nl.mpi.oai.harvester.control.Main"

# ------------------------------------------------------------
# Run harvester
# ------------------------------------------------------------

cd "$CONFIGDIR" || exit 1

java -classpath "$CLASSPATH" $JAVAARGS $MAINCLASS "$CONFIGDIR/$CONFIG" "$@"

