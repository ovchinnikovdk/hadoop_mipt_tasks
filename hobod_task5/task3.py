#!/usr/bin/env python
import re
import sys
from pyspark import SparkContext
from pyspark import SparkConf
from datetime import datetime as dt
from operator import add

log_format = re.compile(
	r"(?P<host>[\d\.]+)\s" 
	r"(?P<identity>\S*)\s" 
	r"(?P<user>\S*)\s"
	r"\[(?P<time>.*?)\]\s"
	r'"(?P<request>.*?)"\s'
	r"(?P<status>\d+)\s"
	r"(?P<bytes>\S*)\s"
	r'"(?P<referer>.*?)"\s'
	r'"(?P<user_agent>.*?)"\s*'
)

def parseLine(line):
	match = log_format.match(line)
	if not match:
		return ("", "", "", "", "", "", "" ,"", "")
	
	request = match.group('request').split()
	return (match.group('host'), match.group('time').split()[0], \
		request[0], request[1], match.group('status'), match.group('bytes'), \
		match.group('referer'), match.group('user_agent'),\
		dt.strptime(match.group('time').split()[0], '%d/%b/%Y:%H:%M:%S').hour)


if __name__ == "__main__":
	conf = SparkConf().setAppName("task3").setMaster(sys.argv[1]).set("spark.ui.port", "4090")
	sc = SparkContext(conf=conf)
	lines = sc.textFile("%s" % sys.argv[2])
	ip_count = lines \
		.map(parseLine) \
		.filter(lambda x : len(x[0]) > 0) \
		.map(lambda x: "%s\t%s\t%s" % (x[0], x[7], x[8])) \
		.distinct() \
		.map(lambda key : (key.split("\t")[2], 1)) \
		.reduceByKey(add) \
		.filter(lambda (key, value) : len(key) > 0 and len(key) < 3) \
		.sortBy(lambda x : x[0], True) \
		.cache().take(5)
	for key,val in ip_count:
		print key, val
