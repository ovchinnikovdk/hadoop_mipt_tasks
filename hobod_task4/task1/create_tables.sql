ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-serde.jar;
ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;

USE s201709;

DROP TABLE IF EXISTS originalLogs;

CREATE EXTERNAL TABLE  originalLogs(
	ip STRING,
	date STRING,
	request STRING,
	clength STRING,
	code STRING,
	client STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = '^(\\S*)\\t\\t\\t(\\S*)\\t(\\S*)\\t(\\S*)\\t(\\S*)\\t(\\S*).*$'
)
STORED AS TEXTFILE
LOCATION '/data/user_logs/user_logs_S';

drop table if exists Logs;

create external table Logs(
	ip string,
	request string,
	clength string,
	code string,
	client string
)
partitioned by (day string);

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstruct;

insert overwrite table Logs
partition(day)
select ip, request, clength, code, client, substring(date, 1, 8) as day from originalLogs;


select * from logs limit 10;

DROP TABLE IF EXISTS IPRegions;

CREATE EXTERNAL TABLE IPRegions(
	ip STRING,
	region STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
	"input.regex" = "^(\\S*)\\t(\\S*).*$"
)
STORED AS TEXTFILE
LOCATION '/data/user_logs/ip_data';

select * from ipregions limit 10;

DROP TABLE IF EXISTS users;

CREATE EXTERNAL TABLE users(
	ip STRING,
	client STRING,
	gender STRING,
	age STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
	"input.regex" = '^(\\S*)\\t(\\S*)\\t(\\S*)\\t(\\S*).*$'
)
STORED AS TEXTFILE
LOCATION '/data/user_logs/user_data_S';

select * from users limit 10;

DROP TABLE IF EXISTS subnets;

CREATE EXTERNAL TABLE subnets(
        ip STRING,
        mask STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
        "input.regex" = '^(\\S*)\\t(\\S*).*$'
)
STORED AS TEXTFILE
LOCATION '/data/subnets/variant2';

select * from subnets limit 10;

