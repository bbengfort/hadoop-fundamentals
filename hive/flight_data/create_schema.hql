CREATE DATABASE IF NOT EXISTS flight_data;

SHOW DATABASES;

USE flight_data;

CREATE TABLE flights (
        flight_date DATE,
        airline_code INT,
        carrier_code STRING,
        origin STRING,
        dest STRING,
        depart_time INT,
        depart_delta INT,
        depart_delay INT,
        arrive_time INT,
        arrive_delta INT,
        arrive_delay INT,
        is_cancelled BOOLEAN,
        cancellation_code STRING,
        distance INT,
        carrier_delay INT,
        weather_delay INT,
        nas_delay INT,
        security_delay INT,
        late_aircraft_delay INT
        )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

DESC ontime_data;

CREATE TABLE airlines (code INT, description STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;

CREATE TABLE carriers (code STRING, description STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;

CREATE TABLE cancellation_reasons (code STRING, description STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;

SHOW TABLES;