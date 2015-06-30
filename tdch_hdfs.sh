#!/bin/bash

# Examples from TDCH 1.3.4 README file section 7.1.1.1
# Data movement between Teradata and HDFS
# Run tdch_setup.sh to set up environment for these functions
# Run this script with the name of the function to execute.  
#
# Load Hadoop file into Teradata table:
#   ./tdch_hdfs.sh hdfs_2
# 
# Copy Teradata table to Hadoop:
#   ./tdch_hdfs.sh hdfs_1

. ./tdch_env.sh

# tdch_env.sh default STFILE resides on a MapR NFS mount.
# This way, we can have a TDCH mapreduce job read it as
# a hadoop file.  If your $STFILE is not in /mapr/<cluster>,
# copy it to a location in the hadoop file system and set
# SOURCEPATH below to the hadoop file location

SOURCEPATH=$STFILE  # Hadoop file to be transferred to Teradata

export TDCH_JAR=/usr/lib/tdch/1.3/lib/teradata-connector-1.3.4.jar

# 1. The following sample commands demonstrate how to import all four
#    columns from the 'sales_transaction' table in Teradata system to
#    an HDFS directory 'sales_transaction':

hdfs_1(){
  
  echo "TDCH: Copy Teradata table $TD_TABLE to Hadoop directory $HADOOP_DATADIR/$TD_TABLE"

  # Remove hdfs directory (if it exists)
  hadoop fs -rm -r $HADOOP_DATADIR/$TD_TABLE

  # Note that targetpaths is in the Hadoop filesystem
  hadoop jar $TDCH_JAR \
    com.teradata.connector.common.tool.ConnectorImportTool \
    -classname com.teradata.jdbc.TeraDriver \
    -url jdbc:teradata://${TD_SERVER}/DATABASE=${TD_USER} \
    -username $TD_USER \
    -password $TD_PWD \
    -jobtype hdfs \
    -fileformat textfile \
    -method split.by.hash \
    -separator "," \
    -sourcetable $TD_TABLE \
    -targetpaths $HADOOP_DATADIR/$TD_TABLE
}

# 2. The following sample commands demonstrate how to export fields
#    stored in path of '/user/$HADOOP_USER/data/stfile' in HDFS to
#    the 'sales_transaction' table in Teradata system:

hdfs_2(){

  echo "TDCH: Load Hadoop file $SOURCEPATH into Teradata table $TD_TABLE"

  hadoop jar $TDCH_JAR \
    com.teradata.connector.common.tool.ConnectorExportTool \
    -classname com.teradata.jdbc.TeraDriver \
    -url jdbc:teradata://${TD_SERVER}/DATABASE=${TD_USER} \
    -username $TD_USER \
    -password $TD_PWD \
    -jobtype hdfs \
    -fileformat textfile \
    -method batch.insert \
    -separator "," \
    -sourcepaths $SOURCEPATH \
    -targettable $TD_TABLE
}

$1
