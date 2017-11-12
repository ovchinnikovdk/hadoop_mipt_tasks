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
		return ("", "", "", "", "", "", "" ,"")
	
	request = match.group('request').split()
	return (match.group('host'), match.group('time').split()[0], \
	request[0], request[1], match.group('status'), match.group('bytes'), \
	match.group('referer'), match.group('user_agent'),
	dt.strptime(match.group('time').split()[0], '%d/%b/%Y:%H:%M:%S').hour)


if __name__ == "__main__":
	conf = SparkConf().setAppName("task1").setMaster(sys.argv[1]).set("spark.ui.port", "4090")
	sc = SparkContext(conf=conf)
	rdd1 = sc.textFile("%s" % sys.argv[2])
	vals = rdd1.map(parseLine)
	rdd2 = vals.map(lambda x : x[0])
	rddFilter = rdd2.filter(lambda x : "7" in x)
	rdd3 = rddFilter.map(lambda x : (x,1))
	rdd5 = rdd3.reduceByKey(add).sortBy(lambda x : x[1], False)
	sortedIp = rdd5.cache().take(5)
	for ip, count in sortedIp:
		print ip, count

