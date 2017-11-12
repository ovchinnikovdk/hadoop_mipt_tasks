ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-serde.jar;
ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;

use s201709;

select gender, logs.client, count(request) 
from users
join logs
on users.ip = logs.ip
group by gender, logs.client;
