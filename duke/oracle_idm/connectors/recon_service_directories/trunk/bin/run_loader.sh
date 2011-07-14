#!/bin/sh

get_pid() {
  PID=`ps -eo pid,cmd | grep edu.duke.oit.idms.oracle.connectors.recon_service_directories.RunConnector | grep -v grep | awk '{print $1}'`
}

export OIM_CONNECTOR_HOME=/srv/idms/oracle_idm/connectors/recon_service_directories
export OIM_COMMON_HOME=/srv/idms/oracle_idm/common

for i in `ls $OIM_CONNECTOR_HOME/lib/*.jar $OIM_COMMON_HOME/lib/*.jar $OIM_COMMON_HOME/util/*.jar`; do CLASSPATH=$CLASSPATH:$i; done

get_pid

if [ $PID ]
then
  echo "Connector already running..."
  exit
fi



nohup java -classpath $CLASSPATH -Xms512m -Xmx512m -DOIM_COMMON_HOME=$OIM_COMMON_HOME -DXL.HomeDir=$OIM_COMMON_HOME/conf/oim -Djava.security.egd=file:///dev/urandom -Djava.security.auth.login.config=$OIM_COMMON_HOME/conf/oim/config/authwl.conf edu.duke.oit.idms.oracle.connectors.recon_service_directories.RunConnector &
