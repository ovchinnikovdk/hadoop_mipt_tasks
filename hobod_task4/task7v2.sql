ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-serde.jar;
ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;

use s201709;

create view task7 as 
select transform (ip, day, request, code, clength, client) 
using '/bin/sed s/http/ftp/'
as ip, day, request, code, clength, client from logs;

select * from task7 limit 10;

