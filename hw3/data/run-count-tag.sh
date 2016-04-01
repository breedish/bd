#!/bin/sh

hdfs dfs -rm -r  /apps/bdc/hw3/output/

DATASET=hdfs://sandbox.hortonworks.com:8020/apps/bdc/hw3/dataset
OUTPUT_DIR=hdfs://sandbox.hortonworks.com:8020/apps/bdc/hw3/output/
hadoop jar target/hw3-1.0.jar com.epam.bdc.tag.CountTagApp -files ./target/user.tags.txt -libjars ./target/lib/UserAgentUtils-1.19.jar $DATASET $OUTPUT_DIR