ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-serde.jar;
ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;

use s201709;

select region, count(request) as visits
from ipregions_optimized
join logs_optimized
on ipregions_optimized.ip = logs_optimized.ip
group by region
order by visits desc;

select region, count(request) as visits
from ipregions
join logs
on ipregions.ip = logs.ip
group by region
order by visits desc;
