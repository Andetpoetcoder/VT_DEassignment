#!/bin/bash

# Start the HDFS NameNode
hdfs namenode &

sleep 10

hdfs dfs -mkdir -p /raw_zone/fact/activity
hdfs dfs -chmod 777 /raw_zone/fact/activity
hdfs dfs -mkdir -p /output_zone
hdfs dfs -chmod 777 /output_zone
hdfs dfs -put /Hadoop/danh_sach_sv_de.csv hdfs://namenode/raw_zone/fact/student_list

tail -f /dev/null
