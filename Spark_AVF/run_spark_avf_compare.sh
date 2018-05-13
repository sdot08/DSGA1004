#!/bin/sh
# Source global definitions
if [ -f /etc/bashrc ]; then
        . /etc/bashrc
fi

# User specific aliases and functions
HADOOP_EXE='/usr/bin/hadoop'
HADOOP_LIBPATH='/opt/cloudera/parcels/CDH/lib'
HADOOP_STREAMING='hadoop-mapreduce/hadoop-streaming.jar'
alias hfs="$HADOOP_EXE fs"
alias hjs="$HADOOP_EXE jar $HADOOP_LIBPATH/$HADOOP_STREAMING"
export PYSPARK_PYTHON='/share/apps/python/3.4.4/bin/python'

export PYSPARK_DRIVER_PYTHON='/share/apps/python/3.4.4/bin/python'
module load python/gnu/3.4.4

module load spark/2.2.0


START=$(date +%s.%N)

spark-submit \
--conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python \
mravf.py cat-r4s5-tb2g.tsv 3

hfs -getmerge cat-r4s5-tb2g_mroutliers.out cat-r4s5-tb2g_mroutliers

END=$(date +%s.%N)

DIFF=$(echo "$END - $START" | bc)
echo $DIFF