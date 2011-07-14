#!/bin/sh
export OIM_APP_HOME=/srv/idms/oracle_idm/utilities/update_oim_users/trunk
export OIM_COMMON_HOME=/srv/idms/oracle_idm/common/trunk

  java -jar -Xms128m -Xmx128m -Djava.security.egd=file:///dev/urandom -DXL.HomeDir=$OIM_COMMON_HOME/conf/oim -Djava.security.auth.login.config=$OIM_COMMON_HOME/conf/oim/config/authwl.conf $OIM_APP_HOME/lib/update_oim_users.jar $*
