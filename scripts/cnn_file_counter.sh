#!/bin/bash

# Shell script to scan file system directory and count the number of
# CNN transcript in each month. 

# List all files out to a file
find . -name \*.html > filelist.tmp

# From 2000 to 2012
for yy in {00..12}
do
	# From Jan-Dec
	for mm in {01..12} 
	do
		cat filelist.tmp | grep $yy$mm | wc -l
	done
done

# Clean up the file lst
rm filelist.tmp