#hdfs dfs -put 37it-gmcp.tsv /user/ys3202/
from pyspark import SparkContext
import sys
from csv import reader
import csv
#increase the maximum field length
maxInt = sys.maxsize
decrement = True

while decrement:
	decrement = False
	try:
		csv.field_size_limit(maxInt)
	except OverflowError:
		maxInt = int(maxInt/2)
		decrement = True

sc = SparkContext('local', 'avf')
data = sc.textFile(sys.argv[1], 3)
f = sys.argv[1][:-4]
#data = sc.textFile('/user/ys3202/cat-w3c6-35wg.tsv', 3)

#default percent_outliers 
percent_outliars = 5
#take percent_outliars from the user
percent_outliars = sys.argv[2]
#calculate number of outliers the algorithm produce
num_outliers = float(percent_outliars) / 100.0 * data.count()

data = data.mapPartitions(lambda x: reader(x, delimiter = '\t', quoting=csv.QUOTE_NONE))
header = data.first() #extract header
index = 0
#find out the column of the index
for i in range(len(header)):
	if header[i] == 'appended_index':
		index = i
#delete header
data = data.filter(lambda row: row != header)

num_cols = len(header) - 1  #number of attributes
#map data to (key=index, val= data)
data = data.map(lambda x: (x[index],x))

#detect null in data
def filterNA(y, num_cols):
	for i in range(num_cols):
		if y[i] == ' ' or y[i] == 'N/A' or y[i] == '':
			return True
	return False


null_row = data.filter(lambda x: filterNA(x[1], num_cols))
result_null = null_row.map(lambda x: "index = " + str(x[0]) + '\n' + str(x[1]))
#output null to txt file
result_null.saveAsTextFile(str(f) + "_null.out")

#delete null data
data = data.filter(lambda x: not filterNA(x[1], num_cols))

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
	data_c = data.map(lambda x: ((x[1][i],i), 1))
	#sum the value correpond to the same key
	freq_c = data_c.reduceByKey(lambda x, y : x + y)
	#update dict_freq with freq_c
	dictupdate((dict_freq), dict(freq_c.collect()))

#convert categorical value with the frequency of that value
def cat2freq(x, num_cols):
	output = []
	for i in range(num_cols):
		output.append(dict_freq[(x[i], i)])
	return output

data_freq = data.map(lambda x: (x[0],x[1], cat2freq(x[1], num_cols-1)))



#sum the frequency of all attributes for each obs
data_avf = data_freq.map(lambda x: (sum(x[2]), (x[0],x[1])))

#sort data according to its avf score
data_avf_sorted = data_avf.sortByKey()

#find the top k outliars, k = num_outliers
outliers = data_avf_sorted.take(int(num_outliers))
#write outliers into results
result = []
for i in range(len(outliers)):
	result.append('avf = ' + str(outliers[i][0]) + '\n' + str(outliers[i][1][1]))

result = sc.parallelize(result)

result.saveAsTextFile(str(f) + "_outliers.out")


