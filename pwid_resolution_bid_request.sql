-- PWIDs (from hh and resolved) by household 

WITH dim_bidstream as (
select * from ozone.dim_bidstream
where -- DATE(ts) = current_date - 1
ts BETWEEN TIMESTAMP("2024-12-12T16:00:00") AND TIMESTAMP("2024-12-12T17:00:00")
AND user_country = 'United Kingdom'
),

hhip_eu as (
  SELECT 
  DISTINCT
  date AS date_h,
  ip_address_prefix
  FROM `ozone-analytics-dev.ozone.fct_ids__ip_address_prefix_mapping`
  LEFT JOIN UNNEST(user_ad_platform_ids) AS user_ad_platform_ids
  WHERE 
  date = current_date - 1
  AND app_region = 'eu'
  AND order_tier <= 55000000
),

hhip_us as (
  SELECT 
  DISTINCT
  date AS date_h,
  ip_address_prefix
  FROM `ozone-analytics-dev.ozone.fct_ids__ip_address_prefix_mapping`
  LEFT JOIN UNNEST(user_ad_platform_ids) AS user_ad_platform_ids
  WHERE 
  date = current_date - 1
  AND app_region = 'us'
  AND order_tier <= 76000000
),

-- unfold dim_bidstream to seat level 

seat_level_raw as (
  SELECT
  DATE(ts) as date_ts,
  case
  when regexp_contains(ip_address, r"^([0-9]{1,3}\.){3}[0-9]{1,3}$") then ip_address --ipv4
  when regexp_contains(ip_address, r"^([A-Za-z0-9]*\:){1,7}[A-Za-z0-9]*$") then NET.IP_TO_STRING(NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(ip_address), 64)) --ipv6
  end as ip_address,
  device_name, 
  os_name,
  browser_name,
  ozone_user.user_id as ozone_id,
  user_ids,
  request_id,
  ad_id,
  seat_name,
  seat.is_bid_request_sent as is_bid_request_sent,
  (SELECT MAX(TRUE) FROM UNNEST(seat.experiment) e WHERE e.name = 'b_id' AND e.type = 'd_ig') AS is_b_id_d_ig,
  (SELECT MAX(TRUE) FROM UNNEST(seat.experiment) e WHERE e.name = 'b_id' AND e.type = 'ctr') AS is_b_id_ctr,
  (SELECT MAX(br.is_impression) FROM UNNEST(bid_request) br) AS br_is_impression,
  (SELECT MAX(br.bid_net_usd) FROM UNNEST(bid_request) br WHERE br.bid_net_usd IS NOT NULL AND br.is_impression = TRUE) AS br_max_bid_net_usd
  FROM dim_bidstream
  LEFT JOIN UNNEST(ad) AS ad
  LEFT JOIN UNNEST(seat) AS seat
  WHERE (seat_name LIKE '%beeswax%' OR seat_name LIKE 'ozbwomp%')
),


-- expand different lookup keys prior to looking up the graph

cached_hhip_pwid AS (
  SELECT 
  date_ts,
  (SELECT user_id_entry.user_id FROM UNNEST(user_ids) as user_id_entry WHERE user_id_entry.id_type = 'pubcid' LIMIT 1)  AS pubcid_id,
  (SELECT user_id_entry.user_id FROM UNNEST(user_ids) as user_id_entry WHERE user_id_entry.id_type = 'anonymous-id' LIMIT 1)  AS anonymous_id,
  (SELECT user_id_entry.user_id FROM UNNEST(user_ids) as user_id_entry WHERE user_id_entry.id_type = 'login-id' LIMIT 1)  AS login_id,
  (SELECT user_id_entry.user_id FROM UNNEST(user_ids) as user_id_entry WHERE user_id_entry.id_type = 'maid' LIMIT 1)  AS maid_id,
  ozone_id,
  device_name, 
  os_name, 
  browser_name,
  request_id,
  ad_id,
  seat_name,
  is_bid_request_sent,
  is_b_id_d_ig,
  is_b_id_ctr,
  br_is_impression,
  br_max_bid_net_usd
  FROM seat_level_raw
), 

-- pull keys to map non-hhips to hhips

-- create table with the concatenation of HHIPs cached in login, anonymous, maid, pubcid and ozone id tables 

cached_hhip AS (

    SELECT date, user_id_login_hash as mapping_key, user_ad_platform_ids.user_ad_platform_id 
    FROM `ozone-analytics-dev.ozone.fct_ids__user_id_login_hash_mapping` 
    LEFT JOIN UNNEST(user_ad_platform_ids) as user_ad_platform_ids 
    WHERE date = current_date - 1 AND user_ad_platform_ids.ad_platform = 'hhip'
    UNION ALL
    SELECT date, user_id_anonymous_hash as mapping_key, user_ad_platform_ids.user_ad_platform_id 
    FROM `ozone-analytics-dev.ozone.fct_ids__user_id_anonymous_hash_mapping` 
    LEFT JOIN UNNEST(user_ad_platform_ids) as user_ad_platform_ids 
    WHERE date = current_date - 1 AND user_ad_platform_ids.ad_platform = 'hhip'
    UNION ALL 
    SELECT date, user_id_maid as mapping_key, user_ad_platform_ids.user_ad_platform_id 
    FROM `ozone-analytics-dev.ozone.fct_ids__user_id_maid_mapping` 
    LEFT JOIN UNNEST(user_ad_platform_ids) as user_ad_platform_ids 
    WHERE date = current_date - 1 AND user_ad_platform_ids.ad_platform = 'hhip'
    UNION ALL
    SELECT date, user_id_pubcid as mapping_key, user_ad_platform_ids.user_ad_platform_id 
    FROM `ozone-analytics-dev.ozone.fct_ids__user_id_pubcid_mapping` 
    LEFT JOIN UNNEST(user_ad_platform_ids) as user_ad_platform_ids 
    WHERE date = current_date - 1 AND user_ad_platform_ids.ad_platform = 'hhip'
    UNION ALL
    SELECT date, user_id_ozone as mapping_key, user_ad_platform_ids.user_ad_platform_id 
    FROM `ozone-analytics-dev.ozone.fct_ids__user_id_ozone_mapping` 
    LEFT JOIN UNNEST(user_ad_platform_ids) as user_ad_platform_ids 
    WHERE date = current_date - 1 AND user_ad_platform_ids.ad_platform = 'hhip'

),

-- map non hhips to what's cached in mapping tables

map_cached_hhip_pwid AS (

  SELECT 
  date_ts,
  "cached_hhip" AS ip_source,
  CONCAT(ch.user_ad_platform_id, device_name, os_name, browser_name) AS premium_web_id, -- pwid
  request_id,
  ad_id,
  seat_name,
  is_bid_request_sent,
  is_b_id_d_ig,
  is_b_id_ctr,
  br_is_impression,
  br_max_bid_net_usd
  FROM cached_hhip_pwid
  INNER JOIN cached_hhip ch ON ch.mapping_key = to_hex(md5(login_id))
                         OR ch.mapping_key = anonymous_id
                         OR ch.mapping_key = maid_id
                         OR ch.mapping_key = pubcid_id
                         OR ch.mapping_key = ozone_id
),

hhip_pwid as (
  SELECT 
  date_ts,
  'request_hhip' as ip_source,
  CONCAT(ip_address, os_name, device_name, browser_name) AS premium_web_id, -- pwid
  request_id,
  ad_id,
  seat_name,
  is_bid_request_sent,
  is_b_id_d_ig,
  is_b_id_ctr,
  br_is_impression,
  br_max_bid_net_usd
  FROM seat_level_raw
  INNER JOIN hhip_eu ON ip_address_prefix = ip_address AND date_h = date_ts
  WHERE request_id NOT IN (SELECT DISTINCT request_id FROM map_cached_hhip_pwid) -- only include request ids that have not been mapped before
  group by all
),


ozone_pwid as (
    SELECT
    date_ts,
    'no_hhip_found' as ip_source,
    ozone_id as premium_web_id,
    request_id,
    ad_id,
    seat_name,
    is_bid_request_sent,
    is_b_id_d_ig,
    is_b_id_ctr,
    br_is_impression,
    br_max_bid_net_usd
    FROM seat_level_raw
    WHERE request_id NOT IN (SELECT DISTINCT request_id FROM map_cached_hhip_pwid
                            UNION ALL
                            SELECT DISTINCT request_id FROM hhip_pwid) -- only include request ids that have not been mapped before
),

-------------------
-- good until here
-------------------

-- concatenate all pwids

all_pwid as (
    SELECT * FROM map_cached_hhip_pwid
    UNION ALL
    SELECT * FROM hhip_pwid
    UNION ALL
    SELECT * FROM ozone_pwid
),
-- calcualte reach and frequency

final as (
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
        else 'no_exp_bucket'
    end as exp_type,
    count(is_bid_request_sent) as bid_request_sent,
    count(br_is_impression) as imps,
    ROUND(sum(safe_divide(br_max_bid_net_usd,1000)),0) as br_revenue,
    ROUND(SUM(SAFE_DIVIDE(br_max_bid_net_usd, 1000)) / NULLIF(COUNT(is_bid_request_sent), 0) * 1000000, 2) AS bCPMM,
    ROUND(COUNT(br_is_impression) / NULLIF(COUNT(is_bid_request_sent), 0) * 100, 2) AS fill_rate,
    ROUND(SUM(SAFE_DIVIDE(br_max_bid_net_usd, 1000)) / NULLIF(COUNT(br_is_impression), 0) * 1000, 2) AS CPM,
    COUNT(DISTINCT premium_web_id) as unique_pwid
    FROM all_pwid
    GROUP BY all
)

SELECT * FROM final

