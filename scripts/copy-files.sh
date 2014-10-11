#!/bin/bash

# Set Maven home directory
if [ "$#" -eq 1 ]
then
  MVN_HOME=$1
fi

# Test for parameters
if [ -z $MVN_HOME ]
then
  echo "Environment variable MVN_HOME (Maven home directory) must be set. As an alternative, provide MVN_HOME as an argument"
  echo "    e.g. copy-files.sh /opt/maven"
  exit 1
fi

# Set dir
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Set destination properties
DEST_USER=root
DEST_IP=172.17.0.100

# Set Biohadoop jar file
BIOHADOOP_CURRENT=biohadoop-[0-9]*.jar

# Set Biooozie home directory
BIOOOZIE_PROJECT_HOME=$DIR/..

# Set Biooozie version
BIOOOZIE_CURRENT=biooozie-*.jar

# Set Biohadoop-algorithms version (this is only needed if the corresponding jar is used. If you have developed your own algorithms and they are in a jar file, please copy the jar file manually to the hadoop environment and move it there to the directory /opt/oozie/current/oozie-server/webapps/oozie/WEB-INF/lib/
BIOHADOOP_ALGORITHMS_CURRENT=biohadoop-algorithms*.jar

# Set remote lib dirs
LIB_TMP_DIR=/tmp/lib
OOZIE_TMP_DIR=/tmp/oozie
OOZIE_HDFS_DIR=/biohadoop/oozie

function build {
  echo "Building Biooozie with Maven"
  $MVN_HOME/bin/mvn -f $BIOOOZIE_PROJECT_HOME clean install
  if [ "$?" -ne 0 ]
  then
    echo "Error while building Biooozie"
    exit 1
  fi
}

# ssh calls need " because this way the variable is substituted before sending to remote host
function copyLibRemote {
  echo "Copying libs to remote FS"
  scp -r $BIOOOZIE_PROJECT_HOME/target/$BIOOOZIE_CURRENT $DEST_USER@$DEST_IP:$LIB_TMP_DIR
  ssh $DEST_USER@$DEST_IP "cp $LIB_TMP_DIR/$BIOHADOOP_CURRENT /opt/oozie/current/oozie-server/webapps/oozie/WEB-INF/lib/"
  ssh $DEST_USER@$DEST_IP "cp $LIB_TMP_DIR/$BIOOOZIE_CURRENT /opt/oozie/current/oozie-server/webapps/oozie/WEB-INF/lib/"
  
  echo "Trying to copy biohadoop-agorithms. This may succeed or not, depending on your environment. If you have your own algorithms in a jar file, please copy the jar file manually to the hadoop environment and move it there to the directory /opt/oozie/current/oozie-server/webapps/oozie/WEB-INF/lib/"
  ssh $DEST_USER@$DEST_IP "cp $LIB_TMP_DIR/$BIOHADOOP_ALGORITHMS_CURRENT /opt/oozie/current/oozie-server/webapps/oozie/WEB-INF/lib/"
}

function copyDataRemote {
  ssh $DEST_USER@$DEST_IP "rm -rf $OOZIE_TMP_DIR"
  scp -r $BIOOOZIE_PROJECT_HOME/src/main/apps/ $DEST_USER@$DEST_IP:$OOZIE_TMP_DIR
  ssh $DEST_USER@$DEST_IP "/opt/hadoop/current/bin/hdfs dfs -rm -r $OOZIE_HDFS_DIR"
  ssh $DEST_USER@$DEST_IP "/opt/hadoop/current/bin/hdfs dfs -mkdir -p $OOZIE_HDFS_DIR"
  ssh $DEST_USER@$DEST_IP "/opt/hadoop/current/bin/hdfs dfs -copyFromLocal -f $OOZIE_TMP_DIR/* $OOZIE_HDFS_DIR"
}

function restartOozie {
  ssh $DEST_USER@$DEST_IP "/opt/oozie/current/bin/oozied.sh stop"
  sleep 2
  ssh $DEST_USER@$DEST_IP "/opt/oozie/current/bin/oozied.sh start"
}

#### main part ####
build
copyLibRemote
copyDataRemote
restartOozie
