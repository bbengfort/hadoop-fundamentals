CREATE DATABASE IF NOT EXISTS baseball_stats;

USE baseball_stats;

CREATE TABLE IF NOT EXISTS master (
  lahmanID INT,
  playerID STRING,
  managerID STRING,
  hofID STRING,
  birthYear INT,
  birthMonth INT,
  birthDay INT,
  birthCountry STRING,
  birthState STRING,
  birthCity STRING,
  deathYear INT,
  deathMonth INT,
  deathDay INT,
  deathCountry STRING,
  deathState STRING,
  deathCity STRING,
  nameFirst STRING,
  nameLast STRING,
  nameNote STRING,
  nameGiven STRING,
  nameNick STRING,
  weight INT,
  height DOUBLE,
  bats STRING,
  throws STRING,
  debut STRING,
  finalGame STRING,
  college STRING,
  lahman40ID STRING,
  lahman45ID STRING,
  retroID STRING,
  holtzID STRING,
  bbrefID STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS teams (
  yearID INT,
  lgID STRING,
  teamID STRING,
  franchID STRING,
  divID STRING,
  rank INT,
  games_played INT,
  games_played_home INT,
  wins INT,
  losses INT,
  div_win STRING,
  wild_card_win STRING,
  league_win STRING,
  ws_win STRING,
  runs_scored INT,
  at_bats INT,
  hits_by_batters INT,
  doubles INT,
  triples INT,
  homeruns INT,
  walks_by_batters INT,
  strikeouts_batter INT,
  stolen_bases INT,
  caught_stealing INT,
  hit_by_pitch INT,
  sacrifice_flies INT,
  opp_runs INT,
  earned_runs_allowed INT,
  earned_runs_avg DOUBLE,
  complete_games INT,
  shutouts INT,
  saves INT,
  outs_pitched INT,
  hits_allowed INT,
  homeruns_allowed INT,
  walks_allowed INT,
  strikeouts_pitcher INT,
  errors INT,
  double_plays INT,
  fielding_pct DOUBLE,
  name STRING,
  park STRING,
  attendance INT,
  BPF INT,
  PPF INT,
  teamIDBR STRING,
  teamIDlahman45 STRING,
  teamIDretro STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS batting (
  playerID STRING,
  yearID INT,
  stint INT,
  teamID STRING,
  lgID STRING,
  games INT,
  games_as_batter INT,
  at_bats INT,
  runs INT,
  hits INT,
  doubles INT,
  triples INT,
  homeruns INT,
  runs_batted_in INT,
  stolen_bases INT,
  caught_stealing INT,
  base_on_balls INT,
  strikeouts INT,
  intentional_walks INT,
  hit_by_pitch INT,
  sacrifice_hits INT,
  sacrifice_flies INT,
  grounded INT,
  old_version_games INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS salaries (
  yearID INT,
  teamID STRING,
  lgID STRING,
  playerID STRING,
  salary DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '../../data/baseball/Master.csv' OVERWRITE INTO TABLE master;

LOAD DATA LOCAL INPATH '../../data/baseball/Teams.csv' OVERWRITE INTO TABLE teams;

LOAD DATA LOCAL INPATH '../../data/baseball/Salaries.csv' OVERWRITE INTO TABLE salaries;

LOAD DATA LOCAL INPATH '../../data/baseball/Batting.csv' OVERWRITE INTO TABLE batting;
