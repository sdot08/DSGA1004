# ffnc-f3aa.tsv

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from csv import reader
from pyspark import SparkContext
from pyspark.ml.feature import QuantileDiscretizer

sc = SparkContext("local", "cleaner")
spark = SparkSession.builder.master("local").appName("cleaner").config("spark.some.config.option", "some-value").getOrCreate()

data = sc.textFile(sys.argv[1], 1)
data = data.mapPartitions(lambda row: reader(row, delimiter = "\t"))
header = data.first()
data = data.filter(lambda row: row != header)

data = spark.createDataFrame(data, header)

numerical = set()
categorical = set()
thres = data.count() * 0.1

for colname in header:
    # If this col has limited unique vals, we consider it as categorical
    if data.select(colname).distinct().count() < thres:
        categorical.add(colname)
    else:
        # Cast float strs to float, if success, consider it as numerical
        data = data.withColumn(colname, data[colname].cast('float'))
        # Very stupid way to check casting result, should be replaced
        # The reason of doing this is because casting strs like "B" to float won't raise exception but return None instead
        is_numerical = True
        for i in range(5):
            if data.head(5)[i][colname] == None:
                is_numerical = False
                break
        if is_numerical:
            numerical.add(colname)
        else:
            data = data.drop(colname)

if len(numerical) >= len(header) * 0.75:
    data = data.select(list(numerical))
    # Write to csv file

else:
    # Bin numerical columns and print
    for num_col in numerical:
        data = QuantileDiscretizer(numBuckets = 10, inputCol = num_col, outputCol = num_col + "_binned").fit(data).transform(data)
        data = data.drop(num_col)
    # Write to csv file
