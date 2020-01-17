CREATE SCHEMA IF NOT EXISTS flashml;
use flashml;

DROP TABLE flashml.YELP_DATA_10K;

CREATE TABLE IF NOT EXISTS flashml.YELP_DATA_10K
(
review_id string,
text string,
stars Int
);

LOAD DATA LOCAL INPATH 'data/yelp-data/reviews_10k.json.gz' OVERWRITE INTO TABLE flashml.YELP_DATA_10K;


