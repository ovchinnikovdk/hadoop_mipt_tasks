ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-serde.jar;
ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;

use s201709;

select age
from (
select ip, count(request) as visits from logs
group by ip) as table1
join
(select ip, age  from users) as table2
on table1.ip = table2.ip
cross join (select avg(age) as avg_age from users) as table3
where visits < avg_age;	
