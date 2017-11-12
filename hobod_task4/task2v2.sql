ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-serde.jar;
ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;

use s201709;

select day as http_code, count(code) as code_count 
from logs
group by day
order by code_count;
