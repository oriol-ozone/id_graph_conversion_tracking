-- PWIDs (from hh and resolved) by household 

WITH dim_bidstream AS (
  SELECT * 
  FROM ozone.dim_bidstream
  WHERE DATE(ts) = current_date - 1
  --ts BETWEEN TIMESTAMP(DATETIME(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), TIME "00:00:00")) AND TIMESTAMP(DATETIME(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), TIME "12:10:00"))
    AND user_country = 'United Kingdom'
),

-- unfold dim_bidstream to seat level 

seat_level_raw as (
  SELECT
  DATE(ts) as date_ts,
  user_ids,
  request_id,
  ad_id,
  (SELECT MAX(br.bid_creative_id) FROM UNNEST(bid_request) br) as creative_id,
  seat_name,
  seat.is_bid_request_sent as is_bid_request_sent,
  (SELECT MAX(TRUE) FROM UNNEST(seat.experiment) e WHERE e.name = 'b_id' AND e.type = 'd_ig') AS is_b_id_d_ig,
  (SELECT MAX(TRUE) FROM UNNEST(seat.experiment) e WHERE e.name = 'b_id' AND e.type = 'ctr') AS is_b_id_ctr,
  (SELECT br.user_id FROM UNNEST(bid_request) br LIMIT 1) AS beeswax_user_id,
  (SELECT MAX(br.is_impression) FROM UNNEST(bid_request) br) AS br_is_impression,
  (SELECT MAX(br.bid_net_usd) FROM UNNEST(bid_request) br WHERE br.bid_net_usd IS NOT NULL AND br.is_impression = TRUE) AS br_max_bid_net_usd
  FROM dim_bidstream
  LEFT JOIN UNNEST(ad) AS ad
  LEFT JOIN UNNEST(seat) AS seat
  WHERE (seat_name LIKE '%beeswax%' OR seat_name LIKE 'ozbwomp%')
  AND (SELECT MAX(br.is_impression) FROM UNNEST(bid_request) br) IS TRUE -- filter only to look at impression
  AND (SELECT MAX(br.bid_creative_id) FROM UNNEST(bid_request) br) = 'ozone-15038'
),

final as (
    SELECT 
    date_ts,
    creative_id,
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
        else 'no_exp_bucket'
    end as exp_type,
    count(is_bid_request_sent) as bid_request_sent,
    count(br_is_impression) as imps,
    ROUND(sum(safe_divide(br_max_bid_net_usd,1000)),0) as br_revenue,
    ROUND(SUM(SAFE_DIVIDE(br_max_bid_net_usd, 1000)) / NULLIF(COUNT(is_bid_request_sent), 0) * 1000000, 2) AS bCPMM,
    ROUND(COUNT(br_is_impression) / NULLIF(COUNT(is_bid_request_sent), 0) * 100, 2) AS fill_rate,
    ROUND(SUM(SAFE_DIVIDE(br_max_bid_net_usd, 1000)) / NULLIF(COUNT(br_is_impression), 0) * 1000, 2) AS CPM,
    COUNT(DISTINCT beeswax_user_id) as unique_beeswax_user_id
    FROM seat_level_raw
    GROUP BY all
)

SELECT * FROM final

