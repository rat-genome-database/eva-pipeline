#!/usr/bin/env bash
#
# Eva loading pipeline
#
. /etc/profile
APPNAME=EvaPipeline
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`

APPDIR=/home/rgddata/pipelines/$APPNAME
cd $APPDIR

java -Dspring.config=$APPDIR/../properties/default_db2.xml \
    -Dlog4j.configuration=file://$APPDIR/properties/log4j.properties \
    -jar lib/$APPNAME.jar --importEva"$@" > run.log 2>&1

mailx -s "[$SERVER] Eva Pipeline Run" mtutaj@mcw.edu,llamers@mcw.edu < $APPDIR/logs/summary.log
