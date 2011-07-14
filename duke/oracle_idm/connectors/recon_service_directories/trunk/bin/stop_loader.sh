#!/bin/sh

echo "Stopping connector..."

get_pid() {
  PID=`ps -eo pid,cmd | grep edu.duke.oit.idms.oracle.connectors.recon_service_directories.RunConnector | grep -v grep | awk '{print $1}'`
}

get_pid

if [ $PID ]
then
  kill $PID
fi

while [ $PID ]
do
  echo "Waiting for connector to shutdown...";
  sleep 1
  get_pid
done
