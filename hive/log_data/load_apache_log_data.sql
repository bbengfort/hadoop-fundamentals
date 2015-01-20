USE log_data;
LOAD DATA INPATH 'statistics/log_data/apache.log' OVERWRITE INTO TABLE apache_log;
