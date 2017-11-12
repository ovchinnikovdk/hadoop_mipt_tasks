#!/usr/bin/env python

import re
import sys
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *

def inverse_friends(line):
	ids = line.split("{")
	user_id = ids[0].strip()
	return map(lambda x : (x.split(",")[0].replace("(", "").strip(), user_id), ids[1].split(","))
	

if __name__ == "__main__":
	conf = SparkConf().setAppName("task5").setMaster(sys.argv[1]).set("spark.ui.port", "4090")
	sc = SparkContext(conf=conf)
	friends = sc.textFile("%s" % sys.argv[2])
	#sqlContext = SQLContext(sc)
	
	#inverse_friends = friends.flatMap(inverse_friends).cache().take(20)
	#result = result.collect()
	#sortedData = sorted(result, key=lambda x : x[1], reverse=True)[:5]
	for id1, id2 in inverse_friends:
		print id1, id2

