WITH dim_bidstream AS (
SELECT *
FROM ozone.dim_bidstream
WHERE
--ts BETWEEN TIMESTAMP("2024-10-28T16:00:00") AND TIMESTAMP("2024-10-28T17:00:00")
DATE(ts) = CURRENT_DATE - 1
),

seat_level AS (
SELECT
DATE(ts) AS date_ts,
request_id,
ad_id,
seat_name,
seat.is_bid_request_sent,
(SELECT MAX(br.is_impression) FROM UNNEST(bid_request) AS br) AS br_is_impression,
-- Check if test group from b_id is present
(SELECT MAX(TRUE) FROM UNNEST(seat.experiment) AS experiment WHERE experiment.name = 'b_id' AND experiment.type = 'd_ig') AS is_b_id_d_ig,
-- Check if control group is present
(SELECT MAX(TRUE) FROM UNNEST(seat.experiment) AS experiment WHERE experiment.name = 'b_id' AND experiment.type = 'ctr') AS is_b_id_ctr,

--Check the id source for each request
(SELECT MAX(TRUE) FROM UNNEST(seat.bid_request) as bid WHERE bid.id_source_graph = 'anonymous-id') AS id_source_anonymous_id,
(SELECT MAX(TRUE) FROM UNNEST(seat.bid_request) as bid WHERE bid.id_source_graph = 'hhip') AS id_source_hhip,
(SELECT MAX(TRUE) FROM UNNEST(seat.bid_request) as bid WHERE bid.id_source_graph = 'login-id') AS id_source_login_id,
(SELECT MAX(TRUE) FROM UNNEST(seat.bid_request) as bid WHERE bid.id_source_graph = 'maid') AS id_source_maid,
(SELECT MAX(TRUE) FROM UNNEST(seat.bid_request) as bid WHERE bid.id_source_graph = 'pubcid') AS id_source_pubcid,
(SELECT MAX(TRUE) FROM UNNEST(seat.bid_request) as bid WHERE bid.id_source_graph is null AND bid.is_user_id is True) AS id_source_cookie,

-- Max bid net USD for impressions without grouping
CASE when max_bid_net_usd IS NOT NULL AND seat.is_impression IS TRUE THEN max_bid_net_usd ELSE 0 END as max_bid_net_usd,
(SELECT MAX(br.bid_net_usd) FROM UNNEST(bid_request) AS br WHERE br.bid_net_usd IS NOT NULL AND br.is_impression IS TRUE) AS br_max_bid_net_usd
FROM
dim_bidstream
LEFT JOIN
UNNEST(ad) AS ad
LEFT JOIN
UNNEST(seat) AS seat
)

SELECT
date_ts,
CASE
WHEN seat_name = 'beeswax' THEN 'beeswax'
WHEN seat_name = 'ozbeeswax' THEN 'beeswax'
WHEN seat_name = 'ozbeeswaxv' THEN 'beeswax'
WHEN seat_name = 'o