#!/bin/sh

MAILTO=shilen@duke.edu

export OIM_APP_HOME=/srv/idms/oracle_idm/utilities/prov_tasks
export OIM_COMMON_HOME=/srv/idms/oracle_idm/common

for i in `ls $OIM_APP_HOME/lib/*.jar $OIM_COMMON_HOME/lib/*.jar $OIM_COMMON_HOME/util/*.jar`; do export CLASSPATH=$CLASSPATH:$i; done

java -Xms128m -Xmx128m -DXL.HomeDir=$OIM_COMMON_HOME/conf/oim -Djava.security.egd=file:///dev/urandom -Djava.security.auth.login.config=$OIM_COMMON_HOME/conf/oim/config/authwl.conf edu.duke.oit.idms.oracle.prov_tasks.ProvTaskExecute $1 $2 "$3";
