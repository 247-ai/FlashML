CREATE SCHEMA IF NOT EXISTS flashml;
use flashml;

DROP TABLE flashml.WEB_JOURNEY_DATA;

CREATE TABLE IF NOT EXISTS flashml.WEB_JOURNEY_DATA
(
    dt string,
    vid string,
    active_session INT,
    browser string,
    os string,
    os_cat string,
    isp string,
    browser_cat string,
    dd string,
    city string,
    region string,
    country string,
    referrer string,
    initial_referrer_cat string,
    internal_search_flag INT,
    external_search_flag INT,
    top_current Float,
    purchase_flag INT,
    page_count INT,
    nop_till_purchase INT,
    nop_till_proactive_accept INT,
    nop_till_button_accept INT,
    nop_count INT,
    proactive_chat_all_accept_ri string,
    button_chat_all_accept_ri string,
    nop_till_proactive_offer INT,
    proactive_chat_all_offer_ri string,
    nop_till_button_offer INT,
    button_chat_all_offer_ri string,
    current_page_type string,
    current_page_url string,
    current_page_nav_path string,
    first_proactive_offer INT,
    first_button_offer INT,
    first_proactive_accept INT,
    first_button_accept INT,
    proactive_offer_flag INT,
    button_offer_flag INT,
    proactive_chat_accept_flag INT,
    button_chat_accept_flag INT,
    proactive_chat_all_offer_tos string,
    button_chat_all_offer_tos string,
    proactive_chat_all_accept_tos string,
    button_chat_all_accept_tos string,
    rv float,
    session_start_server string,
    no_of_button_accepts_in_last_7_days INT,
    no_of_proactive_accepts_in_last_7_days INT,
    no_of_visits_in_last_7_days INT,
    no_of_purchases_in_last_7_days INT,
    session_time Float,
    hour_of_day INT,
    repeatVisitor INT
)
COMMENT 'Web Journey Data'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH 'data/web_journey_data.tsv.gz' OVERWRITE INTO TABLE flashml.WEB_JOURNEY_DATA;


