This repository contains scripts that implement examples from the Teradata
Connector for Hadoop (TDCH) README file.  

tdch_env.sh	
-----------
Defines environment variables to specify the Teradata database server, as well
as user names and passwords.  It also contains a function for creating the 
sample sales transaction data file for the examples.  All of the other provided
scripts source this script to set their environment.  You must export the 
environment variables defined in this script (or modify the default values) to 
correspond to your setup.

tdch_setup.sh	
-------------
Creates a user and sales transaction table on the Teradata database.  Also 
creates the sales transaction data file.

tdch_hdfs.sh	
------------
TDCH hdfs examples from the README file.  Each function hdfs_N corresponds 
to example N in section 7.1.1.1 of the TDCH README file.  tdch_setup.sh must be
run before any functions in tdch_hdfs.sh are executed.

tdch_hive.sh	
------------
TDCH hive examples from the README file.  Each function hive_N corresponds 
to example N in section 7.1.1.2 of the TDCH README file.  tdch_setup.sh must be 
run before any functions in tdch_hive.sh are executed.

tdch_cleanup.sh	
---------------
Drop the Teradata table and user created by tdch_setup.sh.  

