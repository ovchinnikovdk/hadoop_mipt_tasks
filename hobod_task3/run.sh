#!/usr/bin/env bash

hadoop fs -rm -r out_hw3 tmp
hadoop jar hadoop-streaming.jar \
	-files mapper.py,reducer.py \
	-input /data/user_events \
	-output tmp \
	-mapper "python mapper.py" \
	-reducer "python reducer.py"
hadoop jar hadoop-streaming.jar \
	-D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator\
	-D mapred.text.key.comparator.options='-k2rn' \
	-D stream.num.map.output.key.fields=3 \
	-numReduceTasks 1 \
	-input tmp \
	-output out_hw3 \
	-mapper cat \
	-reducer cat \

hadoop fs -cat out_hw3/part-00000 | head -10
