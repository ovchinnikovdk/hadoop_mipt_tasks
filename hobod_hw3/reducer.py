#!/usr/bin/env python

import sys

current_key = None

worktime_count = 0
resttime_count = 0

for line in sys.stdin:
	values = line.split()
	key = values[0]
	if (key != current_key):
		if (current_key != None):
			print "%s\t%d\t%d" % (current_key, worktime_count, resttime_count)
		worktime_count = 0
		resttime_count = 0
		current_key = key
	day = int(values[2])
	hour = int (values[1])
	if (day <= 4):
		if (hour >= 7 and hour <= 18):
			worktime_count += 1
		else:
			resttime_count += 1
if (worktime_count != 0 or resttime_count != 0):
	print "%s\t%d\t%d" % (current_key, worktime_count, resttime_count)
