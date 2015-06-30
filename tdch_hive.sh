#!/bin/bash

# Examples from TDCH 1.3.4 README file section 7.1.1.2
# Data movement between Teradata and Hive
# Run tdch_setup.sh to set up environment for these functions
# Run this script with the name of the function to execute.  
# Assumptions:
# 1. tdch_setup.sh has completed successfully
# 2. Hiveserver2 is running on server $HIVESERVER2 with impersonation enabled.
# 3. Hive is installed on the local server in $HIVE_HOME
# 4. /user/hive/warehouse is open for writing by the $HADOOP_USER
#
# Import Teradata table to Hadoop rcfile format:
#   ./tdch_hive.sh hive_1
#
# Import and create a non-partitioned rcfile hive table from 3 columns of a Teradata table
#   ./tdch_hive.sh hive_2
#
# Import data from Teradata to an existing hive table
#   ./tdch_hive.sh hive_3
#
# Export data from Hive table to Teradata table
#   ./tdch_hive.sh hive_4
#
# Export data from Hive partitioned table to Teradata table
#   ./tdch_hive.sh hive_5
#

. ./tdch_env.sh

HIVE_WAREHOUSE=$(hadoop fs -ls /user/hive | grep /warehouse$)
if ! echo "$HIVE_WAREHOUSE" | grep ^drwxrwxrw ; then
  echo "Permissions not open for hive warehouse:"
  echo "    $HIVE_WAREHOUSE"
  echo "As user root or user mapr, set permission with the following command:" 
  echo "    hadoop fs -chmod 1777 /user/hive/warehouse"
  exit
fi

LOCALDATAFILE=$STFILE
TD_FASTLOADHOST=$(hostname)

BEELINE="${HIVE_HOME}/bin/beeline \
  -u jdbc:hive2://$HIVESERVER2:10000 \
  -n $HADOOP_USER \
  -p $HADOOP_PWD \
  -d org.apache.hive.jdbc.HiveDriver"

export TDCH_JAR=/usr/lib/tdch/1.3/lib/teradata-connector-1.3.4.jar

export HADOOP_CLASSPATH=$(hadoop classpath):$HIVE_HOME/conf
for JAR in $HIVE_HOME/lib/*.jar; do
  export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$JAR
  export HIVE_LIB_JARS=$HIVE_LIB_JARS,$JAR
done
export HIVE_LIB_JARS=${HIVE_LIB_JARS#,}

# 1. This following sample commands demonstrate how to import all four
#    columns from a 'sales_transaction' table in Teradata system to
#    Hive, store the Hive-compatible file contents in a non-partitioned
#    directory, but not create a Hive table at the end of import:

hive_1() {
  echo "TDCH: Remove the Hive sales_transaction table data from the Hive warehouse"
  hadoop fs -rm -r /user/hive/warehouse/sales_transaction
  echo "TDCH: Import 4 columns from Teradata table $TD_TABLE to the Hive warehouse"
  hadoop jar $TDCH_JAR \
    com.teradata.connector.common.tool.ConnectorImportTool \
    -libjars $HIVE_LIB_JARS \
    -classname com.teradata.jdbc.TeraDriver \
    -url jdbc:teradata://${TD_SERVER}/DATABASE=$TD_USER \
    -username $TD_USER \
    -password $TD_PWD \
    -jobtype hive \
    -fileformat rcfile \
    -method split.by.hash \
    -sourcetable $TD_TABLE \
    -targetpaths /user/hive/warehouse/sales_transaction \
    -targettableschema "tran_id int,tran_date string,customer string, \
                         amount float" 
  echo "sales_transaction table data created:"
  hadoop fs -ls /user/hive/warehouse/sales_transaction
}

# 2. This following sample commands demonstrate how to import three
#    columns from a 'sales_transaction' table in Teradata system to Hive
#    and create a non-partitioned table in Hive at the end of import:

hive_2() {
  echo "TDCH: Drop hive sales_transaction table"
  $BEELINE -e "drop table sales_transaction;"
  
  echo "TDCH: Import 3 columns from Teradata table $TD_TABLE to hive sales_transaction table"
  hadoop jar $TDCH_JAR \
    com.teradata.connector.common.tool.ConnectorImportTool \
    -libjars $HIVE_LIB_JARS \
    -classname com.teradata.jdbc.TeraDriver \
    -url jdbc:teradata://${TD_SERVER}/DATABASE=$TD_USER \
    -username ${TD_USER}\
    -password ${TD_PWD} \
    -jobtype hive \
    -fileformat rcfile \
    -method split.by.value \
    -sourcetable $TD_TABLE \
    -sourcefieldnames "tran_date,customer,amount" \
    -targettable sales_transaction \
    -targettableschema "tran_date string,customer string,amount float" \
    -targetfieldnames "tran_date,customer,amount" \

  # $BEELINE -e "select count(*) from sales_transaction;"
}

# 3. This following sample commands demonstrate how to import three
#    columns from a 'sales_transaction' table in Teradata system and add
#    to an existing Hive table:
hive_3()
{
  echo "TDCH: Drop and create Hive table sales_transaction"
  $BEELINE -e "drop table sales_transaction;"
  $BEELINE -e "create table sales_transaction ( 
  		 tran_date string, 
                 customer string,
  		 amount float
               )
               stored as rcfile;"
 
  echo "TDCH: Import 3 columns from Teradata table $TD_TABLE to Hive table sales_transaction"
  hadoop jar $TDCH_JAR \
    com.teradata.connector.common.tool.ConnectorImportTool \
    -libjars $HIVE_LIB_JARS \
    -classname com.teradata.jdbc.TeraDriver \
    -url jdbc:teradata://${TD_SERVER}/DATABASE=$TD_USER \
    -username $TD_USER \
    -password $TD_PWD \
    -jobtype hive \
    -fileformat rcfile \
    -method split.by.hash \
    -sourcetable $TD_TABLE \
    -sourcefieldnames "tran_date,customer,amount" \
    -targettable sales_transaction \
    -targetfieldnames "tran_date,customer,amount" 
    
   #hadoop fs -ls /user/hive/warehouse/sales_transaction
}

# 4. The following sample commands demonstrate how to export data from
#    a Hive non-partitioned table to the 'sales_transaction' table in
#    Teradata system:

hive_4() {
  echo "TDCH: Drop and load Hive table sales_transaction with data from $LOCALDATAFILE"
  $BEELINE -e "drop table sales_transaction;"
  $BEELINE -e "create table sales_transaction ( 
                 tran_id int, 
  		 tran_date string, 
  		 customer string,
  		 amount float
  		 ) 
               row format delimited fields terminated by ',' 
               stored as textfile;"
  
  create_stfile
  LOADCMD="load data local inpath '"$LOCALDATAFILE"' into table sales_transaction;"
  $BEELINE -e "$LOADCMD"
  
  #echo "sales_transaction hive table:"
  #$BEELINE -e "select * from sales_transaction;"
  echo "TDCH: Export 3 columns from hive table sales_transaction to Teradata table $TD_TABLE"
  hadoop jar $TDCH_JAR \
    com.teradata.connector.common.tool.ConnectorExportTool \
    -libjars $HIVE_LIB_JARS \
    -classname com.teradata.jdbc.TeraDriver \
    -url jdbc:teradata://$TD_SERVER/DATABASE=$TD_USER \
    -username $TD_USER \
    -password $TD_PWD \
    -jobtype hive \
    -fileformat textfile \
    -method internal.fastload \
    -fastloadsockethost $TD_FASTLOADHOST \
    -fastloadsocketport 8678 \
    -separator "," \
    -sourcedatabase default \
    -sourcetable sales_transaction \
    -sourcefieldnames "tran_id,customer,amount" \
    -targettable $TD_TABLE \
    -targetfieldnames "tran_id,customer,amount"  
}

# 5. The following sample commands demonstrate how to export data from
#    a Hive partitioned table to the 'sales_transaction' table in
#    Teradata system:

hive_5() {
  echo "TDCH: Drop and load hive table sales_transaction_p with data from sales_transaction table"
  $BEELINE -e "drop table sales_transaction_p;"
  $BEELINE -e "create table sales_transaction_p ( 
                   tran_id int, 
  		   tran_date string, 
  		   amount float
  		 ) 
                 partitioned by (customer string)
               stored as rcfile;"
  $BEELINE -e "insert into table sales_transaction_p 
                           partition (customer)
               select tran_id, tran_date, amount, customer 
               from sales_transaction;" \
            --hiveconf hive.exec.dynamic.partition.mode=nonstrict
  
  echo "TDCH: Export 3 columns from Hive table sales_transaction_p to Teradata table $TD_TABLE"
  hadoop jar $TDCH_JAR \
    com.teradata.connector.common.tool.ConnectorExportTool \
    -libjars $HIVE_LIB_JARS \
    -classname com.teradata.jdbc.TeraDriver \
    -url jdbc:teradata://$TD_SERVER/DATABASE=$TD_USER \
    -username $TD_USER \
    -password $TD_PWD \
    -jobtype hive \
    -fileformat rcfile \
    -method internal.fastload \
    -fastloadsockethost $TD_FASTLOADHOST \
    -fastloadsocketport 8678 \
    -sourcetable sales_transaction_p \
    -sourcefieldnames "tran_id,customer,amount" \
    -targettable $TD_TABLE \
    -targetfieldnames "tran_id,customer,amount" 
}

$1
