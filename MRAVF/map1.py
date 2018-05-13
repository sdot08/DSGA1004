#!/usr/bin/python

 
import sys
import string


# input comes from STDIN (stream data that goes to the program)

for line in sys.stdin:
    
	#Remove leading and trailing whitespace
	line = line.strip()

	#Split line into array of entry data
	entry = line.split("\t")
	len_entry = len(entry)
	for i in range(0,len_entry - 1):
		key = (str(entry[i]), i-1)	
		print(str(entry[i]) + '$$' + str(i) + '\t' + str(entry[-1]))
 
	
        
