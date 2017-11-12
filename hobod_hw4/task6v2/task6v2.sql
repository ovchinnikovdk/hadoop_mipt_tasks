ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-serde.jar;
ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;
ADD JAR jar/MyUDF.jar;

use s201709;

create temporary function megabyte as 'MyUdf';

select megabyte(clength) from logs limit 10;
