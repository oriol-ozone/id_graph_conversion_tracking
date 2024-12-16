-- PWIDs (from hh and resolved) by household 

WITH dim_bidstream AS (
  SELECT * 
  FROM ozone.dim_bidstream
  WHERE ts BETWEEN TIMESTAMP(DATETIME(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), TIME "16:00:00"))
              AND TIMESTAMP(DATETIME(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), TIME "16:10:00"))
    AND user_country = 'United Kingdom'
    AND RAND() < 0.01
),

hhip_eu as (
  SELECT 
  date AS date_h,
  ip_address_prefix
  FROM `ozone-analytics-dev.ozone.fct_ids__ip_address_prefix_mapping`
  LEFT JOIN UNNEST(user_ad_platform_ids) AS user_ad_platform_ids
  WHERE 
  date = current_date - 1
  AND app_region = 'eu'
  AND order_tier <= 55000000
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
  AND (SELECT MAX(br.is_impression) FROM UNNEST(bid_request) br) IS TRUE -- filter only to look at impression
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
  ip_address,
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
    (
        SELECT date, user_id_login_hash as mapping_key, user_ad_platform_ids.user_ad_platform_id 
        FROM `ozone-analytics-dev.ozone.fct_ids__user_id_login_hash_mapping`
        LEFT JOIN UNNEST(user_ad_platform_ids) as user_ad_platform_ids 
        WHERE date = current_date - 1 AND user_ad_platform_ids.ad_platform = 'hhip'
        LIMIT 15000000
    )
    UNION ALL
    (
        SELECT date, user_id_anonymous_hash as mapping_key, user_ad_platform_ids.user_ad_platform_id 
        FROM `ozone-analytics-dev.ozone.fct_ids__user_id_anonymous_hash_mapping`
        LEFT JOIN UNNEST(user_ad_platform_ids) as user_ad_platform_ids 
        WHERE date = current_date - 1 AND user_ad_platform_ids.ad_platform = 'hhip'
        LIMIT 15000000
    )
    UNION ALL 
    (
        SELECT date, user_id_maid as mapping_key, user_ad_platform_ids.user_ad_platform_id 
        FROM `ozone-analytics-dev.ozone.fct_ids__user_id_maid_mapping`
        LEFT JOIN UNNEST(user_ad_platform_ids) as user_ad_platform_ids 
        WHERE date = current_date - 1 AND user_ad_platform_ids.ad_platform = 'hhip'
        LIMIT 15000000
    )
    UNION ALL
    (
        SELECT date, user_id_pubcid as mapping_key, user_ad_platform_ids.user_ad_platform_id 
        FROM `ozone-analytics-dev.ozone.fct_ids__user_id_pubcid_mapping`
        LEFT JOIN UNNEST(user_ad_platform_ids) as user_ad_platform_ids 
        WHERE date = current_date - 1 AND user_ad_platform_ids.ad_platform = 'hhip'
        LIMIT 15000000
    )
    UNION ALL
    (
        SELECT date, user_id_ozone as mapping_key, user_ad_platform_ids.user_ad_platform_id 
        FROM `ozone-analytics-dev.ozone.fct_ids__user_id_ozone_mapping`
        LEFT JOIN UNNEST(user_ad_platform_ids) as user_ad_platform_ids 
        WHERE date = current_date - 1 AND user_ad_platform_ids.ad_platform = 'hhip'
        LIMIT 15000000
    )
),

-- Map non-HHIPs to what's cached in mapping tables
map_cached_hhip_pwid AS (
  SELECT 
    date_ts,
    "cached_hhip" AS ip_source,
    CONCAT(ch.user_ad_platform_id, device_name, os_name, browser_name) AS premium_web_id, -- pwid
    ozone_id,
    ip_address,
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
  FROM cached_hhip_pwid
  LEFT JOIN cached_hhip ch 
    ON ch.mapping_key = to_hex(md5(login_id))
    OR ch.mapping_key = anonymous_id
    OR ch.mapping_key = maid_id
    OR ch.mapping_key = pubcid_id
    OR ch.mapping_key = ozone_id
),

-- Use LEFT JOIN to filter out already mapped request_ids
hhip_pwid AS (
  SELECT 
    mch.date_ts,
    'request_hhip' AS ip_source,
    CONCAT(mch.ip_address, mch.os_name, mch.device_name, mch.browser_name) AS premium_web_id, -- pwid
    mch.ozone_id,
    mch.request_id,
    mch.ad_id,
    mch.seat_name,
    mch.is_bid_request_sent,
    mch.is_b_id_d_ig,
    mch.is_b_id_ctr,
    mch.br_is_impression,
    mch.br_max_bid_net_usd
  FROM (SELECT * FROM map_cached_hhip_pwid WHERE premium_web_id IS NULL) mch
  LEFT JOIN hhip_eu ON hhip_eu.ip_address_prefix = mch.ip_address AND hhip_eu.date_h = mch.date_ts
),

-- Assign ozone_id to remaining unmapped request_ids
ozone_pwid AS (
  SELECT
    hp.date_ts,
    'no_hhip_found' AS ip_source,
    hp.ozone_id AS premium_web_id,
    hp.request_id,
    hp.ad_id,
    hp.seat_name,
    hp.is_bid_request_sent,
    hp.is_b_id_d_ig,
    hp.is_b_id_ctr,
    hp.br_is_impression,
    hp.br_max_bid_net_usd
  FROM (SELECT * FROM hhip_pwid WHERE premium_web_id IS NULL) hp
),

-- Concatenate all pwids
all_pwid AS (
  SELECT 
    date_ts,
    ip_source,
    premium_web_id,
    request_id,
    ad_id,
    seat_name,
    is_bid_request_sent,
    is_b_id_d_ig,
    is_b_id_ctr,
    br_is_impression,
    br_max_bid_net_usd 
    FROM map_cached_hhip_pwid
    WHERE premium_web_id IS NOT NULL
  UNION ALL
  SELECT   
    date_ts,
    ip_source,
    premium_web_id,
    request_id,
    ad_id,
    seat_name,
    is_bid_request_sent,
    is_b_id_d_ig,
    is_b_id_ctr,
    br_is_impression,
    br_max_bid_net_usd 
    FROM hhip_pwid
    WHERE premium_web_id IS NOT NULL
  UNION ALL
  SELECT 
    * FROM ozone_pwid
)

SELECT * FROM all_pwid
