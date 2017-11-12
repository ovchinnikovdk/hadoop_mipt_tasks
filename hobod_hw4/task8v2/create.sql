ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-serde.jar;
ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;

use s201709;

drop table if exists logs_optimized;

CREATE TABLE  logs_optimized(
  ip STRING,
  day STRING,
  request STRING,
  length STRING,
  code STRING,
  client STRING
)
STORED AS RCFILE;

insert into table logs_optimized 
select * from logs;

drop table if exists ipregions_optimized;

create table ipregions_optimized (
	ip STRING,
	region STRING
)
stored as RCFILE;

insert into table ipregions_optimized
select * from ipregions;

drop table if exists timestamp_start;
drop table if exists timestamp_fin;
create table timestamp_start (
	name string,
	timestamp decimal
);
create table timestamp_fin (
	name string,
	timestamp decimal
);

insert into table timestamp_start
select "opt", unix_timestamp();

drop table if exists tmp_opt;
create table tmp_opt as
select region, count(request) as visits
from ipregions_optimized
join logs_optimized
on ipregions_optimized.ip = logs_optimized.ip
group by region
order by visits desc;

insert into table timestamp_fin
select "opt", unix_timestamp();

insert into table timestamp_start
select "comm", unix_timestamp();

drop table if exists tmp_comm;
create table tmp_comm as
select region, count(request) as visits
from ipregions
join logs
on ipregions.ip = logs.ip
group by region
order by visits desc;

insert into table timestamp_fin
select "comm", unix_timestamp();


select (timestamp_fin.timestamp - timestamp_start.timestamp) from 
timestamp_start
join timestamp_fin 
on timestamp_start.name = timestamp_fin.name;


