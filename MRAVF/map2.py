#!/usr/bin/python

 
import sys
import string


# input comes from STDIN (stream data that goes to the program)

for line in sys.stdin:
    
	#Remove leading and trailing whitespace
	line = line.strip()

	key, value = line.split('\t',1)
	key_arr = key.split("$$")
	val_arr = value.split("$$", 1)
	col = key_arr[1]
	freq = val_arr[0]

	indice_list = val_arr[1]
	indice_list = indice_list.strip(']').strip('[').split(',')
	
	for i in indice_list:
		i = i.strip()
		print(i + '\t'  + col + '$$' + freq)

 
	
        
