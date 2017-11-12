ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-serde.jar;
ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;

use s201709;

drop table if exists sampled;

create table sampled as
select "10" as percent, gender, logs.client as client, count(request) as cnt 
from users tablesample(10 percent)
join logs tablesample(10 percent)
on users.ip = logs.ip
group by gender, logs.client
union all
select "20" as percent, gender, logs.client as client, count(request) as cnt
from users tablesample(30 percent)
join logs tablesample(30 percent)
on users.ip = logs.ip
group by gender, logs.client
union all
select "50" as percent, gender, logs.client as client, count(request) as cnt
from users tablesample(50 percent)
join logs tablesample(50 percent)
on users.ip = logs.ip
group by gender, logs.client
union all
select "70" as percent, gender, logs.client as client, count(request) as cnt
from users tablesample(70 percent)
join logs tablesample(70 percent)
on users.ip = logs.ip
group by gender, logs.client
union all
select "90" as percent, gender, logs.client as client, count(request) as cnt
from users tablesample(90 percent)
join logs tablesample(90 percent)
on users.ip = logs.ip
group by gender, logs.client;

drop table if exists expected;

create table expected as
select "100" as percent, gender, logs.client as client, count(request) as cnt
from users tablesample(100 percent)
join logs tablesample(100 percent)
on users.ip = logs.ip
group by gender, logs.client;

select sampled.percent, avg(expected.cnt - sampled.cnt) as accuracy
from sampled
left join
expected
on expected.gender = sampled.gender and expected.client = sampled.client
group by sampled.percent;

