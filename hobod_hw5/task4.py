#!/usr/bin/env python
import re
import sys
from pyspark import SparkContext
from pyspark import SparkConf
from datetime import datetime as dt
from operator import add
from pyspark.sql import *
from pyspark.sql.types import *

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
		request[0], parse_url(request[1]), match.group('status'), match.group('bytes'), \
		parse_url(match.group('referer')), match.group('user_agent'),\
		dt.strptime(match.group('time').split()[0], '%d/%b/%Y:%H:%M:%S').hour)

user_id_format = re.compile(
        r".*(?P<id>\/id[\d]+)\s*"
)

def parse_url(url):
	match = user_id_format.match(url)
	if not match:
		return ""
	return match.group('id')


if __name__ == "__main__":
	conf = SparkConf().setAppName("task4").setMaster(sys.argv[1]).set("spark.ui.port", "4090")
	sc = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)
	lines = sc.textFile("%s" % sys.argv[2])
	logs = lines.map(parseLine)
	field_names = ['host', 'time', 'request_type', 'request', 'status', 'bytes', 'referer', 'user_agent', 'hour']
	fields = [StructField(field_name, StringType(), True) for field_name in field_names]
	schema = StructType(fields)
	df = sqlContext.createDataFrame(logs, schema)
	top_100_users = df \
		.select('host', 'user_agent') \
		.groupBy('host', 'user_agent') \
		.count().alias('user_count') \
		.orderBy('count', ascending=False) \
		.limit(100)
	top5_visits = df[df.request.like('/id%')] \
		.select('host', 'user_agent', 'request') \
		.join(top_100_users, [top_100_users.host == df.host, top_100_users.user_agent == df.user_agent]) \
		.select('request') \
		.groupBy('request') \
		.count() \
		.orderBy('count', ascending=False) \
		.limit(5) \
		.collect()

	for request, count in top5_visits:
		print request, count
