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

geolog_format = re.compile(
	r'"(?P<host>[\d\.]+)"\,\s'
	r'"(?P<country>.*?)"\s*'
)


def parseGeologs(line):
	match = geolog_format.match(line)
	if not match:
		return ("", "")
	country = match.group('country').split(":")[-1].split(",")[-1].lstrip()
	return (match.group('host'), country)

if __name__ == "__main__":
	conf = SparkConf().setAppName("task1").setMaster(sys.argv[1]).set("spark.ui.port", "4090")
	sc = SparkContext(conf=conf)
	rdd1 = sc.textFile("%s" % sys.argv[2])
	vals = rdd1.map(parseLine)
	rdd2 = vals.map(lambda x : "%s\t%s" % (x[0],x[7]))
	rdd3 = rdd2.map(lambda x : (x,1)).reduceByKey(add)
	ipcount = rdd3.map(lambda (x,v) : (x.split("\t")[0], v))
	geolog = sc.textFile("%s" % sys.argv[3])
	geolog = geolog.map(parseGeologs)
	result = ipcount.join(geolog).map(lambda (ip, count): (count[1], 1)).reduceByKey(add).sortBy(lambda x : x[1], False).cache().take(5)
	for country, count in result:
		print country, count

