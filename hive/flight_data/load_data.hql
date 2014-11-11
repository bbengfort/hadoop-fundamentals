LOAD DATA INPATH '/user/student/flight_data/input/ontime_flights.tsv' OVERWRITE INTO TABLE flights;

LOAD DATA LOCAL INPATH '${env:HOME}/hadoop-fundamentals/data/flight_data/airlines.tsv' OVERWRITE INTO TABLE airlines;

LOAD DATA LOCAL INPATH '${env:HOME}/hadoop-fundamentals/data/flight_data/carriers.tsv' OVERWRITE INTO TABLE carriers;

LLOAD DATA LOCAL INPATH '${env:HOME}/hadoop-fundamentals/data/flight_data/cancellation_reasons.tsv' OVERWRITE INTO TABLE cancellation_reasons;