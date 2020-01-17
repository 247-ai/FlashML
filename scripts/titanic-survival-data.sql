CREATE SCHEMA IF NOT EXISTS flashml;
use flashml;

DROP TABLE flashml.TITANIC_SURVIVAL_DATA;

CREATE TABLE IF NOT EXISTS flashml.TITANIC_SURVIVAL_DATA
(
  pclass string,
  survived int,
  name string,
  sex string,
  age Float,
  sibsp string,
  parch string,
  ticket string,
  fare Float,
  cabin string,
  embarked string,
  boat string,
  body string,
  home_dest string
)
COMMENT 'Titanic Survival Data'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH 'data/titanic-survival-data.csv.gz' OVERWRITE INTO TABLE flashml.TITANIC_SURVIVAL_DATA;