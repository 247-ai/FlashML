CREATE SCHEMA IF NOT EXISTS flashml;
use flashml;

DROP TABLE flashml.YELP_DATA_1K;

CREATE TABLE IF NOT EXISTS flashml.YELP_DATA_1K
(
review_id string,
text string,
stars Int
);

LOAD DATA LOCAL INPATH 'data/yelp-data/reviews_1k.json.gz' OVERWRITE INTO TABLE flashml.YELP_DATA_1K;
