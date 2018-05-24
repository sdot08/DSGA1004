# DSGA1004

## Where to see results?
See all results here: <a href="https://drive.google.com/drive/folders/11vk9Bb8C9-Ge0SjPut09yIiDfpMcA83B?usp=sharing">Google Drive</a>

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

#### Run Spark AVF
```
./Spark_AVF/run_spark_avf.sh

```
This will output <filename>_outliers and <filename>_null which will contain the null data and outliers of the file.
The default percent of outliers is 5%. It can be changed in run_spark_avf.sh.
  
#### Run MR-AVF
```
./MR_AVF/MRAVF_run.sh
<type the location of the file>
```
This will output answer which will contain the outliers of the file.

#### Compare AFV and MR-AVF
```
./spark_AVF/run_spark_avf_compare.sh
<type the name of the file>
./MR_AVF/MRAVF_run.sh
<type the location of the file>
```
This will output the time taken to implement MR-AVF and Spark AVF respectively. It is worth noting that the input file for MR-AVF have been cleaned by a alternative cleaner cleaner_mr with the header = False.
