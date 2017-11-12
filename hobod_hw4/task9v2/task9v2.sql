ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-serde.jar;
ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;
ADD JAR jar/MyUDTF.jar;

use s201709;

create temporary function generate_ips as 'GenerateIps';

select generate_ips(ip, mask) from subnets;
