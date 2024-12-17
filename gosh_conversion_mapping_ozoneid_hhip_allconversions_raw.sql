WITH dim_bidstream AS (
    SELECT *
    FROM ozone.dim_bidstream
    WHERE  ts BETWEEN TIMESTAMP(DATETIME(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), TIME "06:00:00"))
               AND TIMESTAMP(DATETIME(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), TIME "14:10:00"))
    --DATE(ts) = CURRENT_DATE - 1
    AND user_country = 'United Kingdom'
),
-- pull impressions and spend by IP and ozone id for the GOSH campaign

seat_level_raw AS (
    SELECT
      DATE(ts) AS date_ts,
      request_id,
      ip_address,
      ozone_user.user_id AS ozone_user_id,
      seat_name,
      seat.is_bid_request_sent,
      (SELECT MAX(br.is_impression) FROM UNNEST(bid_request) br) AS br_is_impression,
      (SELECT MAX(br.bid_net_usd) FROM UNNEST(bid_request) br WHERE br.bid_net_usd IS NOT NULL AND br.is_impression = TRUE) AS br_max_bid_net_usd
      FROM dim_bidstream
      LEFT JOIN UNNEST(ad) AS ad
      LEFT JOIN UNNEST(seat) AS seat
      WHERE (seat_name LIKE '%beeswax%' OR seat_name LIKE 'ozbwomp%')
      AND (SELECT MAX(br.is_impression) FROM UNNEST(bid_request) br) IS TRUE -- filter only to look at impressions
      AND (SELECT MAX(br.bid_creative_id) FROM UNNEST(bid_request) br) = 'ozone-15038' -- filter only to capture GOSH campaign
),

-- bring in hhip table for eu

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

site_events AS (
    SELECT *
    FROM `ozone-analytics-dev.ozone.stg_ozone__site_events_1_0_1`
    WHERE 
    --ts BETWEEN TIMESTAMP("2024-12-09T18:00:00") AND TIMESTAMP("2024-12-09T19:00:00")
    DATE(ts) = CURRENT_DATE - 1
),

conversions_raw AS (
    SELECT 
      DATE(ts) AS date_ts,
      ip_address,
      user_ozone_id AS ozone_user_id,
      pixel_id,
      pixel_name
    FROM site_events
    WHERE internal_advertiser_id = '621'
),

-- first add the is_household flag

is_household AS (
  SELECT 
    date_ts,
    request_id,
    ozone_user_id,
    ip_address,
    seat_name,
    is_bid_request_sent,
    br_is_impression,
    br_max_bid_net_usd,
    CASE 
      WHEN hhip_eu.ip_address_prefix IS NULL then FALSE
      ELSE TRUE
    END as is_household
    FROM seat_level_raw
    LEFT JOIN hhip_eu ON hhip_eu.ip_address_prefix = ip_address AND hhip_eu.date_h = date_ts
),

-- first try to join by ozone_user_id
joined_by_user_id AS (
    SELECT
      sl.date_ts,
      sl.request_id,
      sl.seat_name,
      sl.is_bid_request_sent,
      sl.br_is_impression,
      sl.br_max_bid_net_usd,
      CASE 
        WHEN c.pixel_id = '10114' THEN 'GOSH_All_Site_Visitors'
        ELSE NULL
      END as conversion_All_Site_Visitors,
      CASE 
        WHEN c.pixel_id = '10116' THEN 'GOSH_Donation_Started'
        ELSE NULL
      END as conversion_Donation_Started,
      CASE 
        WHEN c.pixel_id = '10115' THEN 'GOSH_Successful_Donation'
        ELSE NULL
      END as conversion_Successful_Donation,
      sl.ozone_user_id AS ozone_user_id,
      sl.ip_address AS ip_address,
      is_household,
      CASE 
        WHEN c.pixel_id = '10114' THEN 'ozone_id' 
        ELSE null 
      END as conversion_mapping_source,
    FROM is_household sl
    LEFT JOIN conversions_raw c USING (ozone_user_id)
),

-- the try to join by ip_address (all of them)

joined_by_hhip AS (
    SELECT
    juid.date_ts as date_ts,
    juid.request_id as request_id,
    juid.seat_name as seat_name,
    juid.is_bid_request_sent as is_bid_request_sent,
    juid.br_is_impression as br_is_impression,
    juid.br_max_bid_net_usd as br_max_bid_net_usd,
    CASE 
        WHEN c.pixel_id = '10114' AND is_household is TRUE THEN 'GOSH_All_Site_Visitors'
        ELSE NULL
    END as conversion_All_Site_Visitors,
    CASE 
        WHEN c.pixel_id = '10116' AND is_household is TRUE THEN 'GOSH_Donation_Started'
        ELSE NULL
    END as conversion_Donation_Started,
    CASE 
        WHEN c.pixel_id = '10115' AND is_household is TRUE THEN 'GOSH_Successful_Donation'
        ELSE NULL
    END as conversion_Successful_Donation,
    juid.ozone_user_id AS ozone_user_id,
    juid.ip_address AS ip_address,
    is_household,
    CASE 
      WHEN c.pixel_id = '10114' AND is_household is TRUE THEN 'hhip' 
      ELSE null 
    END as conversion_mapping_source,
  FROM (SELECT * FROM joined_by_user_id WHERE conversion_All_Site_Visitors IS NULL) juid
  LEFT JOIN conversions_raw c ON c.ip_address = juid.ip_address

),

-- join both tables 

all_conversions AS (
SELECT * from joined_by_user_id WHERE conversion_All_Site_Visitors IS NOT NULL
UNION ALL
SELECT * from joined_by_hhip
),

-- Final aggregation to consolidate pixel data and merge duplicates
final_conversions AS (
  SELECT
    date_ts,
    request_id,
    seat_name,
    is_bid_request_sent,
    br_is_impression,
    br_max_bid_net_usd,
    ozone_user_id,
    ip_address,
    is_household,
    -- Aggregate all mapped pixel names into arrays
    ARRAY_AGG(DISTINCT conversion_All_Site_Visitors IGNORE NULLS) AS all_site_visitors_pixels,
    ARRAY_AGG(DISTINCT conversion_Donation_Started IGNORE NULLS) AS donation_started_pixels,
    ARRAY_AGG(DISTINCT conversion_Successful_Donation IGNORE NULLS) AS successful_donation_pixels,
    ARRAY_AGG(DISTINCT conversion_mapping_source IGNORE NULLS) AS conversion_sources
  FROM all_conversions
  GROUP BY
    date_ts, request_id, seat_name, is_bid_request_sent, br_is_impression,
    br_max_bid_net_usd, ozone_user_id, ip_address, is_household
)

SELECT * FROM final_conversions
