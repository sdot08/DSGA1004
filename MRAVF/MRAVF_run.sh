#!/bin/sh
# Source global definitions
import sys
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
read f

START=$(date +%s.%N)

hjs -D mapreduce.job.reduces=2 -files "map1.py,reduce1.py" -mapper "map1.py" -reducer "reduce1.py"  -input  $f -output /user/ys3202/task1.out



hfs -getmerge task1.out task1
hdfs dfs -put task1 /user/ys3202/

hjs -D mapreduce.job.reduces=2 -files "map2.py,reduce2.py" -mapper "map2.py" -reducer "reduce2.py"  -input  /user/ys3202/task1 -output /user/ys3202/task2.out 

hfs -getmerge task2.out task2
hdfs dfs -put task2 /user/ys3202/

spark-submit \
--conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python \
sort.py /user/ys3202/task2 3

hfs -getmerge avf_result.out answer

END=$(date +%s.%N)

DIFF=$(echo "$END - $START" | bc)
echo $DIFF