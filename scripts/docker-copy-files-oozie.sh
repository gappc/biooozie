#!/bin/bash

if [ -z $1 ]
then
	echo "Usage: docker-copy-files-oozie.sh CONFIG_FILE"
	echo "    where CONFIG_FILE contains needed configuration parameters"
	exit 1
fi

# load configuration from file
source $1

PROJECT_HOME=$BIOOOZIE_PROJECT_HOME

function build {
  $MVN_HOME/bin/mvn -f $PROJECT_HOME clean install
}

# ssh calls need " because this way the variable is substituted before sending to remote host
function copyLibRemote {
  scp -r $PROJECT_HOME/target/$BIOOOZIE_CURRENT root@172.17.0.100:/tmp
  ssh root@172.17.0.100 "cp /tmp/$BIOHADOOP_CURRENT /opt/oozie/current/oozie-server/webapps/oozie/WEB-INF/lib/"
  ssh root@172.17.0.100 "cp /tmp/$BIOOOZIE_CURRENT /opt/oozie/current/oozie-server/webapps/oozie/WEB-INF/lib/"
}

function copyDataRemote {
  scp -r $PROJECT_HOME/src/main/apps/ root@172.17.0.100:/tmp
  ssh root@172.17.0.100 '/opt/hadoop/current/bin/hdfs dfs -rm -r -f /user/root/examples/apps/biohadoop'
  ssh root@172.17.0.100 '/opt/hadoop/current/bin/hdfs dfs -mkdir -p /user/root/examples/apps/biohadoop/lib'
  ssh root@172.17.0.100 '/opt/hadoop/current/bin/hdfs dfs -mkdir -p /user/root/examples/apps/biohadoop/ga'
  ssh root@172.17.0.100 '/opt/hadoop/current/bin/hdfs dfs -mkdir -p /user/root/examples/apps/biohadoop/moead'
  ssh root@172.17.0.100 '/opt/hadoop/current/bin/hdfs dfs -mkdir -p /user/root/examples/apps/biohadoop/nsgaii'
  ssh root@172.17.0.100 '/opt/hadoop/current/bin/hdfs dfs -copyFromLocal -f /tmp/apps/ga/* /user/root/examples/apps/biohadoop/ga'
  ssh root@172.17.0.100 '/opt/hadoop/current/bin/hdfs dfs -copyFromLocal -f /tmp/apps/moead/* /user/root/examples/apps/biohadoop/moead'
  ssh root@172.17.0.100 '/opt/hadoop/current/bin/hdfs dfs -copyFromLocal -f /tmp/apps/nsgaii/* /user/root/examples/apps/biohadoop/nsgaii'
}

function restartOozie {
  ssh root@172.17.0.100 '/opt/oozie/current/bin/oozied.sh stop'
  sleep 2
  ssh root@172.17.0.100 '/opt/oozie/current/bin/oozied.sh start'
}

#### main part ####
build
copyLibRemote
copyDataRemote
restartOozie
