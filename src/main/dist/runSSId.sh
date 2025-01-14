#!/usr/bin/env bash
#
# Eva loading pipeline
#
. /etc/profile
APPNAME=EvaPipeline
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`

EMAILLIST=mtutaj@mcw.edu,llamers@mcw.edu

APPDIR=/home/rgddata/pipelines/$APPNAME
cd $APPDIR

java -Dspring.config=$APPDIR/../properties/default_db2.xml \
    -Dlog4j.configurationFile=file://$APPDIR/properties/log4j2.xml \
    -Xmx20g -jar lib/$APPNAME.jar --importEvaSSIds"$@" > ssIdRun.log 2>&1

mailx -s "[$SERVER] Eva Pipeline Run" $EMAILLIST < $APPDIR/logs/ssIdSummary.log
