#!/bin/sh

MAILTO=shilen@duke.edu

export OIM_APP_HOME=/srv/idms/oracle_idm/utilities/scheduled_file_feeds
export OIM_COMMON_HOME=/srv/idms/oracle_idm/common

cd $OIM_APP_HOME;

for i in `ls $OIM_APP_HOME/lib/*.jar $OIM_APP_HOME/util/*.jar $OIM_COMMON_HOME/lib/*.jar $OIM_COMMON_HOME/util/*.jar`; do export CLASSPATH=$CLASSPATH:$i; done

java -Xms512m -Xmx2048m edu.duke.oit.idms.oracle.scheduled_file_feeds.ScheduledFileFeeds $1

if [ $? -ne 0 ]
then
  echo "Received an unexpected error while executing the Java application." | mail -s "Failed to successfully run feed - $1" $MAILTO
fi
