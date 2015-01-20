CREATE DATABASE IF NOT EXISTS log_data;

USE log_data;

ADD JAR /srv/hive/lib/hive-serde-0.13.1.jar;

CREATE TABLE apache_log (
   host STRING,
   identity STRING,
   user STRING,
   time STRING,
   request STRING,
   status STRING,
   size STRING,
   referer STRING,
   agent STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
  WITH SERDEPROPERTIES (
     "input.regex" = "([^ ]*) ([^ ]*) ([^ ]*) (-|\\[[^\\]]*\\]) ([^ \"]*|\"[^\"]*\") (-|[0-9]*) (-|[0-9]*)(?: ([^ \"]*|\".*\") ([^ \"]*|\".*\"))?",
     "output.format.string" = "%1$s %2$s %3$s %4$s %5$s %6$s %7$s %8$s %9$s"
  )
STORED AS TEXTFILE;

