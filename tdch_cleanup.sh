#!/bin/bash

# Clean up environment for TDCH examples deleting database, files
# Assumptions:
# 1. Teradata database is running on $TD_SERVER
# 2. User dbc has password dbc with privilege to drop $TD_USER
# 3. Passwordless ssh is configured from $HADOOP_USER to $TD_ADMIN user on $TD_SERVER

. ./tdch_env.sh

# Drop database from Teradata 
drop_td_database() {
  cat <<- EOF | ssh $TD_ADMIN@$TD_SERVER "cat | bteq"
	* If user $TD_USER does not exist, ignore failure
	.logon 127.0.0.1/dbc, dbc
	delete user $TD_USER;
	drop database $TD_USER;
	.quit;
EOF
}

# Drop table from Teradata
drop_td_table()
{
  cat <<- EOF | ssh $TD_ADMIN@$TD_SERVER "cat | bteq"
	.logon 127.0.0.1/$TD_USER, $TD_PWD;
        DROP TABLE $TD_TABLE;
	.quit;
EOF
}

# MAIN
drop_td_database
hadoop fs -rm -r $HADOOP_DATADIR \
  || err_exit "tdch_cleanup unable to remove $HADOOP_DATADIR"
