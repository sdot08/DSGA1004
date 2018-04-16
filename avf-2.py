#hdfs dfs -put 37it-gmcp.tsv /user/ys3202/
import sys
from csv import reader
from pyspark import SparkContext
sc = SparkContext('local', 'avf')
data = sc.textFile(sys.argv[1], 1)
#default num_outliers
num_outliars = 3
num_outliars = sys.argv[2]

data = data.mapPartitions(lambda x: reader(x, delimiter = '\t'))
header = data.first() #extract header
data = data.filter(lambda row: row != header) #lose header

num_cols = len(header)  #number of attributes
num_obs = len(data.collect()) #number of observation

#update dict a with dict b
def dictupdate(a, b):
	for key in b:
		if key in a:
			a[key] += b[key]
		else:
			a[key] = b[key]



dict_freq = {}
for i in range(num_cols):
	#map data to key:(x[i], i), value:1 in which i is the number of the column
	data_c = data.map(lambda x: ((x[i],i), 1))
	#sum the value correpond to the same key
	freq_c = data_c.reduceByKey(lambda x, y : x + y)
	#update dict_freq with freq_c
	dictupdate((dict_freq), dict(freq_c.collect()))

#convert categorical value with the frequency of that value
def cat2freq(x):
	output = []
	for i in range(num_cols):
		output.append(dict_freq[(x[i], i)])
	return output

data_freq = data.map(cat2freq)



#sum the frequency of all attributes for each obs
data_avf = data_freq.map(lambda x: sum(x)).collect()
#add index to each obs
def add_index(x):
	output = []
	for i in range(len(x)):
		output.append((x[i],i))
	return output
data_avf = sc.parallelize(add_index(data_avf))
data_avf_sorted = data_avf.sortByKey()

data_df = data.collect()
#find the top 5 outliars
outliers = data_avf_sorted.take(int(num_outliars))
#write outliers into results
result = []
for i in range(len(outliers)):
	result.append("index = " + str(outliers[i][1]) + '\n' + str(data_df[outliers[i][1]]))

result = sc.parallelize(result)
result.saveAsTextFile("avf_result.out")



