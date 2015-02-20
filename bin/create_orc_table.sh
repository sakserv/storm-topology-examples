#!/bin/bash

SQL_FILE=bin/hive/create_orc_table.sql
if [ -z "$1" ]; then
  SQL_FILE="$1"
fi
  
# Create the Hive Table
echo -e "\n#### Executing $SQL_FILE via Hive"
cd /tmp/storm-topology-examples/
hive -f $SQL_FILE