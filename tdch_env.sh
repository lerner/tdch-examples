#!/bin/bash

# Environment variables are set with default values in this file.
# Either modify the default values to match your setup or set
# the values in your environment.  The default values here will
# only be used if the environment variables are not otherwise set.

TD_SERVER=${TD_SERVER:-10.10.99.101}    # Teradata server (ip address or hostname)
TD_DBC_PWD=${TD_DBC_PWD:-dbc}           # Teradata dbc password
TD_USER=${TD_USER:-sampleuser}          # Teradata user
TD_PWD=${TD_PWD:-samplepwd}             # Teradata user's password
TD_TABLE=${TD_TABLE:-sales_transaction} # Teradata table name
TD_ADMIN=${TD_ADMIN:-teradata}          # Teradata server's linux adminstrative user
HADOOP_USER=${HADOOP_USER:-mruser}      # Hadoop linux user
HADOOP_PWD=${HADOOP_USER:-mruser}       # Hadoop linux user's password

# NOTE: The tdch_hdfs.sh script assumes $STFILE is accessible in the hadoop
# filesystem.  MapR's default NFS configuration provides access to files via NFS
# and hadoop APIs using the same file path: /mapr/<clustername>/path_to_hadoop_file.
# If you modify $STFILE to be a local linux file not accessible via a /mapr path, you
# must copy it to the hadoop filesystem and modify SOURCEPATH in tdch_hdfs.sh
# accordingly.

CLUSTERNAME=${CLUSTERNAME:-scale61}     # Cluster name for accessing MapR via NFS
DATADIR=/mapr/${CLUSTERNAME}/user/${HADOOP_USER}/data
STFILE=$DATADIR/stfile                  # Sales transaction text data file

# NOTE: HADOOP_DATADIR is the same directory as DATADIR, just using notation
# without leading /mapr/$CLUSTERNAME to emphasize it is a Hadoop location
# Linux commands must use the /mapr/$CLUSTERNAME notation to access hadoop files
# via NFS, but Hadoop commands can use either notation.
HADOOP_DATADIR=${DATADIR#/mapr/${CLUSTERNAME}}

# The following variables are only used by tdch_hive.sh.  If you are only transferring
# data to/from hadoop files independent of hive, these are not necessary.

export HIVE_HOME=${HIVE_HOME:-/opt/mapr/hive/hive-0.13}
HIVESERVER2=${HIVESERVER2:-scale-64}     # Server running hiveserver2

# create a local data file with sales transaction data
# For convenience, use NFS mounted mapr so we don't need
# to copy to Hadoop later.

create_stfile()
{
  if [[ ! -d $DATADIR ]]; then
    mkdir -p $DATADIR
  fi

  echo "3,2012-11-01,acme3,630.00" > $STFILE
  echo "4,2012-12-01,emca4,760.21" >> $STFILE
}

# Use this function instead of the one above to create a larger stfile
create_stfile_large()
{
  if [[ ! -d $DATADIR ]]; then
    mkdir -p $DATADIR
  fi
  rm -f $STFILE

  RECORDS=10000
  CUSTOMERS=100
  MAXAMT=5000

  for i in $(eval echo {1..$RECORDS}) ; do
    let CUSTNUM=$i%$CUSTOMERS
    let DOLLARS=$i%MAXAMT
    let CENTS=$DOLLARS%100
    let YEAR=2012+$i%4
    let MONTH=1+$i%12
    let DAY=1+$i%31
    CENTS=$(printf "%02d\n" $CENTS)
    MONTH=$(printf "%02d\n" $MONTH)
    DAY=$(printf "%02d\n" $DAY)
    echo "$CUSTNUM,${YEAR}-${MONTH}-${DAY},Customer_XYZ_Limited_$CUSTNUM,${DOLLARS}.${CENTS}" >> $STFILE
  done
}

#
err_exit() {
  printf "TDCH MapR: $@\n"
  exit 1
}
