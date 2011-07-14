#!/bin/sh

MAILTO=shilen@duke.edu

export OIM_APP_HOME=/srv/idms/oracle_idm/utilities/recon_events
export OIM_COMMON_HOME=/srv/idms/oracle_idm/common

for i in `ls $OIM_APP_HOME/lib/*.jar $OIM_COMMON_HOME/lib/*.jar $OIM_COMMON_HOME/util/*.jar`; do export CLASSPATH=$CLASSPATH:$i; done

java -Xms128m -Xmx128m -DXL.HomeDir=$OIM_COMMON_HOME/conf/oim -Djava.security.auth.login.config=$OIM_COMMON_HOME/conf/oim/config/authwl.conf edu.duke.oit.idms.oracle.recon_events.ReconEvents;

if [ $? -ne 0 ]
then
  echo "Received an unexpected error while executing the Java application." | mail -s "Failed to successfully run pending reconciliations report" $MAILTO
fi

