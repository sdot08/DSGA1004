# DSGA1004

## How to execute the whole process
To execute the whole process on NYU Dumbo, make sure that you've logged in and have internet connection.

#### Upload all .tsv to HDFS
Inside the folder where you store all your tsv files, type:
```
hdfs dfs -put *.tsv /user/<netid>/
```
If you want to check if all files are uploaded, type:
```
hdfs dfs -ls /user/<netid>/*.tsv
```
**Don't forget to replace netid with your actual netid, also get rid of larger than and smaller than signs**

#### Clean all tsv files
Type:
```
for f in `hdfs dfs -ls /user/<netid>/*.tsv | grep <netid> | awk '{print $8}'`;do spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python <your file path>/cleaner.py $f; done;
```
Note this will write all files that ready for clustering as num-<orioginal name>.out, and all files ready for AVF as cat-<original name>.out, on your HDFS folder

#### Merge and pull all files to your Dumbo local
Inside where you want to store all processed files:
```
for f in `hdfs dfs -ls -d /user/<netid>/*.out/ | grep <netid> | awk '{print $8}' | xargs -n 1 basename`; do hfs -getmerge $f ${f%.*}$".tsv"; done;
```
This preserves the original .out file name. For example, ABC.out is merged to ABC.tsv 
