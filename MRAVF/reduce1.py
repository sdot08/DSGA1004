#!/usr/bin/python





import sys
import string

# input comes from STDIN (stream data that goes to the program)
current_key = None
num = 0.0
index = []
for line in sys.stdin:
	
	#Remove leading and trailing whitespace
	line = line.strip() 
	#Get key/value 
	key, value = line.split('\t',1)
	value = int(value)
	if key == current_key:
		num += 1
		index.append(value)
	else:
		if current_key:
			print(current_key + '\t' + str(num) + '$$' + str(index))
		current_key = key
		num = 0
		index = []
		num += 1
		index.append(value)
print(current_key + '\t' + str(num) + '$$' + str(index))