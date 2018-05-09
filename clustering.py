# !/usr/bin/env python
import numpy as np
from math import sqrt
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.sql import SparkSession
import sys
spark = SparkSession.builder.appName('cluster').getOrCreate()

#read in system argument
percentage = 5
k_max = 10

#read system input
filename = sys.argv[1].split(".")[0].split("/")[3]


#dataset = spark.read.csv('/user/zh1087/task0merge.tsv', header= True, sep='\t',inferSchema= True )
dataset = spark.read.csv(sys.argv[1], header= True, sep='\t', inferSchema = True)

#######################
#find all null values:

dataset_no_missing = dataset.na.drop()
dataset_missing_rows = dataset.subtract(dataset_no_missing)

#write to hdfs
#dataset_missing_rows.write.csv("null_values.csv", sep=',')

#write to dumbo local:

dataset_missing_rows.toPandas().to_csv(filename+ "_null_values.csv", sep=',')

#######################
#define how many outliar wanted
total_instance = dataset_no_missing.count()

#percentage is system input
num_outliar = int(round(percentage/100 * total_instance))


#transform datatype to be float:
from pyspark.sql.types import FloatType

for i in dataset_no_missing.columns:
    dataset_no_missing = dataset_no_missing.withColumn(i,dataset_no_missing[i].cast(FloatType()))

###############
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=dataset_no_missing.columns, outputCol='features')

final_data = assembler.transform(dataset_no_missing)
final_data.printSchema()


#scaling:
from pyspark.ml.feature import StandardScaler
scaler = StandardScaler(inputCol='features', outputCol = 'scaledFeatures')

scaler_model = scaler.fit(final_data)
final_data = scaler_model.transform(final_data)


###################
#number of clusters and WSSE

WSSE_list = []
k_list = []

for i in range(2,k_max):
    kmeans = KMeans(featuresCol='scaledFeatures',k=i)
    model = kmeans.fit(final_data)
    k_list.append(i)
    WSSE_list.append(model.computeCost(final_data))

#############
#############
#automatically select K

WSSE_list = np.array(WSSE_list)
k_list = np.array(k_list)

secondDerivative = np.array([(WSSE_list[i+1] + WSSE_list[i-1] -2 * WSSE_list[i]) for i in range(1,len(WSSE_list)-1)])

optimal_k = k_list[np.argmax(secondDerivative)+1]

####################
#after selecting k

kmeans = KMeans(featuresCol='scaledFeatures', k= optimal_k)
model = kmeans.fit(final_data)
centers = model.clusterCenters()


print("optimal k is {}".format(optimal_k))
print("WSSSE is {}".format(model.computeCost(final_data)))
print("clustering centers are {}".format(centers))

#model transform final data then have the prediction column
model.transform(final_data).select('prediction').show()

final_data = model.transform(final_data)


#see how many element in each group:
print("element in each group:")
final_data.groupBy('prediction').count().show()

#############
#add column: distance

####previously defined variable can be used in UDF
from pyspark.sql.functions import udf

def cal_distance(val1,val2):
    point = val1
    center = centers[val2]
    return sqrt(sum([x**2 for x in (point - center)]))

udf_cal_distance = udf(cal_distance, FloatType())

df_with_distance = final_data.withColumn("distance", udf_cal_distance("scaledFeatures","prediction"))


####################
#output outliar:

#need to first register dataframe as a SQL temporary view in order to use spark sql
from pyspark.sql.functions import desc

df_with_distance.createOrReplaceTempView("df")

result_df = df_with_distance.sort(desc('distance')).limit(num_outliar)
cols = list(set(result_df.columns)-{'scaledFeatures'}-{'features'})
result_df = result_df.select(cols)

#write to dumbo local:
result_df.toPandas().to_csv(filename+"_numeric_data_result.csv", sep=',')

#write to hdfs
#result_df.write.csv("numeric_data_result.csv", sep=',')

######################
#pca:
from pyspark.ml.feature import PCA
pca = PCA(k=3, inputCol="scaledFeatures", outputCol="pcaFeatures")
#pca_model = pca.fit(final_data)
pca_model = pca.fit(df_with_distance)
#result_pca = pca_model.transform(final_data).select('pcaFeatures','prediction')
result_pca = pca_model.transform(df_with_distance).select('pcaFeatures','prediction','distance')

#will download to dumbo local
result_pca.toPandas().to_csv(filename+"_pca_result.csv") 



