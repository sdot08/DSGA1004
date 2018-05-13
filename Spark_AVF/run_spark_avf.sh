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


for f in `hdfs dfs -ls /user/ys3202/cat-*.tsv | grep ys3202 | awk '{print $8}'`;do spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python avf.py $f 5; done;

for f in `hdfs dfs -ls -d /user/ys3202/*_null*.out/ | grep ys3202 | awk '{print $8}'| xargs -n 1 basename `;do hfs -getmerge $f ${f%.*}; done;

for f in `hdfs dfs -ls -d /user/ys3202/*_outliers*.out/ | grep ys3202 | awk '{print $8}'| xargs -n 1 basename `;do hfs -getmerge $f ${f%.*}; done;