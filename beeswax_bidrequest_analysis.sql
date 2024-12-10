WITH dim_bidstream AS (
    SELECT *
    FROM ozone.dim_bidstream
    WHERE --ts BETWEEN TIMESTAMP("2024-12-09T18:00:00") AND TIMESTAMP("2024-12-09T19:00:00")
    DATE(ts) = CURRENT_DATE - 1
),

seat_level_raw AS (
    SELECT
      DATE(ts) AS date_ts,
      request_id,
      ad_id,
      ip_address,
      browser_name,
      device_type,
      os_name,
      os_version,
      ozone_user.user_id AS ozone_user_id,
      seat_name,
      seat.is_bid_request_sent,
      (SELECT MAX(TRUE) FROM UNNEST(seat.experiment) e WHERE e.name = 'b_id' AND e.type = 'd_ig') AS is_b_id_d_ig,
      (SELECT MAX(TRUE) FROM UNNEST(seat.experiment) e WHERE e.name = 'b_id' AND e.type = 'ctr') AS is_b_id_ctr,
      (SELECT MAX(br.is_impression) FROM UNNEST(bid_request) br) AS br_is_impression,
      (SELECT MAX(br.bid_net_usd) FROM UNNEST(bid_request) br WHERE br.bid_net_usd IS NOT NULL AND br.is_impression = TRUE) AS br_max_bid_net_usd
    FROM dim_bidstream
    LEFT JOIN UNNEST(ad) AS ad
    LEFT JOIN UNNEST(seat) AS seat
    WHERE (seat_name LIKE '%beeswax%' OR seat_name LIKE 'ozbwomp%')
)

-- Aggregate final results
SELECT
  date_ts,
  CASE 
    WHEN seat_name = 'beeswax' THEN 'beeswax'
    WHEN seat_name = 'ozbeeswax' THEN 'beeswax'
    WHEN seat_name = 'ozbeeswaxv' THEN 'beeswax'
    WHEN seat_name = 'ozbeeswaxiv' THEN 'beeswax'
    WHEN seat_name = 'ozbeeswaxibv' THEN 'beeswax'
    WHEN seat_name = 'ozbwomp' THEN 'beeswax'
    WHEN seat_name = 'ozbwompv' THEN 'beeswax'
    WHEN seat_name = 'ozbwompiv' THEN 'beeswax'
    ELSE 'other'
END as ad_platform,
case
    WHEN is_b_id_d_ig IS TRUE THEN 'disable_id_graph'
    WHEN is_b_id_ctr IS TRUE THEN 'enable_id_graph'
    else 'other'
end as exp_type,

count(is_bid_request_sent) as bid_request_sent,
count(br_is_impression) as imps,
ROUND(sum(safe_divide(br_max_bid_net_usd,1000)),0) as br_revenue,
ROUND(SUM(SAFE_DIVIDE(br_max_bid_net_usd, 1000)) / NULLIF(COUNT(is_bid_request_sent), 0) * 1000000, 2) AS bCPMM,
ROUND(COUNT(br_is_impression) / NULLIF(COUNT(is_bid_request_sent), 0) * 100, 2) AS fill_rate,
ROUND(SUM(SAFE_DIVIDE(br_max_bid_net_usd, 1000)) / NULLIF(COUNT(br_is_impression), 0) * 1000, 2) AS CPM

from seat_level_raw

group by all
ORDER BY 
    bCPMM DESC;
