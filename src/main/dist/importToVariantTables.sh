#!/usr/bin/env bash
#
# Eva loading pipeline
#
. /etc/profile
APPNAME=EvaPipeline
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`

EMAILLIST="mtutaj@mcw.edu llamers@mcw.edu"

mapKeys=""
i=1;
j=$#;
while [ $i -le $j ]
do
    mapKeys+=$1
    mapKeys+=" "
    i=$((i + 1));
    shift 1;
done

APPDIR=/home/rgddata/pipelines/$APPNAME
cd $APPDIR

java -Dspring.config=$APPDIR/../properties/default_db2.xml \
    -Dlog4j.configurationFile=file://$APPDIR/properties/log4j2.xml \
    -jar lib/$APPNAME.jar --importVariants $mapKeys"$@" > variantsRun.log 2>&1

mailx -s "[$SERVER] Eva Pipeline Run" $EMAILLIST < $APPDIR/logs/variantSummary.log