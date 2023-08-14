#!/bin/bash

HOMEDIR=$( (cd -P $(dirname $0) && pwd) )

CURRENTDIR=$( (cd -P $(dirname $*) && pwd) )
CONFIG=$(basename $*)

CLASSPATH="$HOMEDIR/target/oai-harvest-manager-1.2.1.jar:$HOMEDIR/target/lib/*"

JAVAARGS=""

MAINCLASS="nl.mpi.oai.harvester.control.Main"

#java -cp ../oai-harvest-manager/target/oai-harvest-manager-1.2.1.8ff86a.jar:../oai-harvest-manager/target/lib nl.mpi.oai.harvester.control.Main  ./config.xml
cd $CURRENTDIR
echo java -classpath $CLASSPATH $JAVAARGS $MAINCLASS $CURRENTDIR/$CONFIG 
java -classpath $CLASSPATH $JAVAARGS $MAINCLASS $CURRENTDIR/$CONFIG 
