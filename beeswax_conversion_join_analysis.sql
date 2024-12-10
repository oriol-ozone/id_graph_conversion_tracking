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
      browser_name,
      device_type,
      os_name,
      os_version,
      user_ozone_id AS ozone_user_id,
      pixel_name
    FROM site_events
    WHERE internal_advertiser_id = '621'
    AND pixel_id = '10115'
),

-- first join by ozone_user_id
joined_by_user_id AS (
    SELECT
      sl.date_ts,
      sl.seat_name,
      sl.is_bid_request_sent,
      sl.br_is_impression,
      sl.is_b_id_d_ig as is_b_id_d_ig,
      sl.is_b_id_ctr as is_b_id_ctr,
      sl.br_max_bid_net_usd,
      c.pixel_name,
      sl.ozone_user_id AS sl_ozone_user_id,
      c.ozone_user_id AS conv_ozone_user_id,
      sl.ip_address AS sl_ip_address,
      sl.browser_name AS sl_browser_name,
      sl.device_type AS sl_device_type,
      sl.os_name AS sl_os_name,
      sl.os_version AS sl_os_version,
      c.ip_address AS conv_ip_address,
      c.browser_name AS conv_browser_name,
      c.device_type AS conv_device_type,
      c.os_name AS conv_os_name,
      c.os_version AS conv_os_version
    FROM seat_level_raw sl
    LEFT JOIN conversions_raw c USING (ozone_user_id)
),

-- identify seat-level rows that still have no conversion match
seat_level_unmatched AS (
    SELECT *
    FROM joined_by_user_id
    WHERE pixel_name IS NULL
),

-- Identify conversions not matched by ozone_user_id
conversions_unmatched AS (
    SELECT c.*
    FROM conversions_raw c
    WHERE c.ozone_user_id NOT IN (
        SELECT DISTINCT conv_ozone_user_id 
        FROM joined_by_user_id 
        WHERE conv_ozone_user_id IS NOT NULL
      )
),

-- Second join match unmatched seat-level rows to unmatched conversions by device

joined_by_device AS (
    SELECT
      slu.date_ts,
      slu.seat_name,
      slu.is_bid_request_sent,
      slu.br_is_impression,
      slu.is_b_id_d_ig as is_b_id_d_ig,
      slu.is_b_id_ctr as is_b_id_ctr,
      slu.br_max_bid_net_usd,
      COALESCE(c.pixel_name, slu.pixel_name) AS pixel_name, -- if c is NULL, keep original (NULL)
      slu.sl_ozone_user_id,
      COALESCE(c.ozone_user_id, slu.conv_ozone_user_id) AS conv_ozone_user_id,
      slu.sl_ip_address,
      slu.sl_browser_name,
      slu.sl_device_type,
      slu.sl_os_name,
      slu.sl_os_version,
      COALESCE(c.ip_address, slu.conv_ip_address) AS conv_ip_address,
      COALESCE(c.browser_name, slu.conv_browser_name) AS conv_browser_name,
      COALESCE(c.device_type, slu.conv_device_type) AS conv_device_type,
      COALESCE(c.os_name, slu.conv_os_name) AS conv_os_name,
      COALESCE(c.os_version, slu.conv_os_version) AS conv_os_version
    FROM seat_level_unmatched slu
    LEFT JOIN conversions_unmatched c
      ON CONCAT(slu.sl_ip_address, slu.sl_browser_name, slu.sl_os_name, slu.sl_os_version) =
         CONCAT(c.ip_address, c.browser_name, c.os_name, c.os_version)
),

-- Combine all results, including unmatched seat_level rows that didn't join in either attempt
final_joined AS (
    SELECT * FROM joined_by_user_id WHERE pixel_name IS NOT NULL -- All rows that were joined with ozone user id
    UNION ALL
    SELECT * FROM joined_by_device WHERE pixel_name IS NOT NULL -- Rows matched by device fingerprint
    UNION ALL
    -- Add back rows from seat_level_unmatched that remain unmatched after both joins
    SELECT
      slu.date_ts,
      slu.seat_name,
      slu.is_bid_request_sent,
      slu.br_is_impression,
      slu.is_b_id_d_ig as is_b_id_d_ig,
      slu.is_b_id_ctr as is_b_id_ctr,
      slu.br_max_bid_net_usd,
      NULL AS pixel_name, -- No match, so these remain NULL
      slu.sl_ozone_user_id,
      NULL AS conv_ozone_user_id,
      slu.sl_ip_address,
      slu.sl_browser_name,
      slu.sl_device_type,
      slu.sl_os_name,
      slu.sl_os_version,
      NULL AS conv_ip_address,
      NULL AS conv_browser_name,
      NULL AS conv_device_type,
      NULL AS conv_os_name,
      NULL AS conv_os_version
    FROM seat_level_unmatched slu
    WHERE pixel_name IS NULL -- Unmatched rows
)

-- Aggregate final results
  SELECT
  date_ts,
  CASE 
    WHEN seat_name IN ('beeswax','ozbeeswax','ozbeeswaxv','ozbeeswaxiv','ozbeeswaxibv','ozbwomp','ozbwompv','ozbwompiv') THEN 'beeswax'
    ELSE 'other'
  END AS ad_platform,
  CASE
    WHEN is_b_id_d_ig IS NOT TRUE AND is_b_id_ctr IS NOT TRUE THEN 'normal_workflow'
    WHEN is_b_id_d_ig IS TRUE THEN 'disable_id_graph'
    WHEN is_b_id_ctr IS TRUE THEN 'enable_id_graph'
    ELSE 'other'
  END AS exp_type,
  pixel_name,
  COUNTIF(is_bid_request_sent) AS bid_request_sent,
  COUNTIF(br_is_impression) AS imps,
  ROUND(SUM(SAFE_DIVIDE(br_max_bid_net_usd,1000)),0) AS br_revenue,
  ROUND(SUM(SAFE_DIVIDE(br_max_bid_net_usd, 1000)) / NULLIF(COUNTIF(is_bid_request_sent), 0) * 1000000, 2) AS bCPMM,
  ROUND(COUNTIF(br_is_impression) / NULLIF(COUNTIF(is_bid_request_sent), 0) * 100, 2) AS fill_rate,
  ROUND(SUM(SAFE_DIVIDE(br_max_bid_net_usd, 1000)) / NULLIF(COUNTIF(br_is_impression), 0) * 1000, 2) AS CPM,
  COUNT(DISTINCT conv_ozone_user_id) AS distinct_ozone_user_id_count,
  COUNT(conv_ozone_user_id) AS total_ozone_user_id_count,
  COUNT(DISTINCT CONCAT(conv_ip_address,conv_browser_name,conv_device_type,conv_os_name,conv_os_version)) AS distinct_pwid_count,
  COUNT(CONCAT(conv_ip_address,conv_browser_name,conv_device_type,conv_os_name,conv_os_version)) AS total_pwid_count
FROM final_joined
GROUP BY ALL
ORDER BY bCPMM DESC;
