import sys
from csv import reader
from pyspark import SparkContext
sc = SparkContext('local', 'avf')
data = sc.textFile(sys.argv[1], 1)
#default num_outliers
num_outliars = 3
num_outliars = sys.argv[2]

data = data.mapPartitions(lambda x: reader(x))

def trans(x):
	a = x[0].split('\t', 1)
	return int(a[1]), int(a[0])

data_1 = data.map(trans)

data_sorted = data_1.sortByKey()
outliers = data_sorted.take(int(num_outliars))
#write outliers into results
result = []
for i in range(len(outliers)):
	result.append("index = " + str(outliers[i][1]) + ', avf = ' + str(outliers[i][0]))

result = sc.parallelize(result)
result.saveAsTextFile("avf_result.out")

