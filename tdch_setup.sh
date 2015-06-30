#!/bin/bash

# Set up Teradata database and table for TDCH examples
# Assumptions:
# 1. Teradata database is running on $TD_SERVER
# 2. Teradata database user $TD_USER has been created OR user dbc has password $TD_DBC_PWD
# 3. Passwordless ssh is configured for $HADOOP_USER to $TD_ADMIN user on $TD_SERVER
# 4. MapR NFS to cluster accessible via /mapr/$CLUSTERNAME
# 5. Linux user $HADOOP_USER is running this script and has write permission 
#    to /mapr/$CLUSTERNAME/user/$HADOOP_USER

. ./tdch_env.sh

TD_SSHOPTS="-o StrictHostKeyChecking=no"
# Create user in Teradata 
create_td_user() {
  echo "TDCH: Logging in to $TD_ADMIN@$TD_SERVER"
  cat <<- EOF | ssh $TD_SSHOPTS $TD_ADMIN@$TD_SERVER "cat | bteq"
	* If user $TD_USER already exists, ignore failure
	.logon 127.0.0.1/dbc, $TD_DBC_PWD
	create user $TD_USER as password=$TD_PWD  perm=524288000 spool=524288000;
	.quit;
EOF
}

# Create table in Teradata 
create_td_table() {
  create_td_user
  cat <<- EOF | ssh $TD_SSHOPTS $TD_ADMIN@$TD_SERVER "cat | bteq"
	* If $TD_TABLE already exists, ignore failure
	.logon 127.0.0.1/$TD_USER, $TD_PWD;
	CREATE MULTISET TABLE $TD_TABLE (
	    tran_id INTEGER,
	    tran_date DATE,
	    customer VARCHAR(100),
	    amount DECIMAL(18,2)
	);
	.quit;
EOF
}

validate() {
  if [[ ! -d $DATADIR ]]; then
    mkdir -p $DATADIR || err_exit "Make sure /mapr is mounted and user $HADOOP_USER has write permission to ${DATADIR%/data}"
  fi
}

# MAIN
validate
create_td_table
create_stfile

