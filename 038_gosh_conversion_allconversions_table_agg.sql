WITH date_interval AS (
    -- Define the base interval for dim_bidstream
    SELECT 
        DATE_SUB(CURRENT_DATE(), INTERVAL 23 DAY) AS dim_start_date,
        DATE_SUB(CURRENT_DATE(), INTERVAL 23 DAY) AS site_events_start_date,
        DATE_SUB(CURRENT_DATE(), INTERVAL 23 DAY) AS site_events_end_date,
        TIME "06:00:00" AS start_time,
        TIME "14:10:00" AS end_time
),

site_events AS (
    SELECT *
    FROM `ozone-analytics-dev.ozone.stg_ozone__site_events_1_0_1`, date_interval
    WHERE DATE(ts) BETWEEN date_interval.site_events_start_date AND date_interval.site_events_end_date
),

conversions_raw AS (
    SELECT 
      DATE(ts) AS date_ts,
      ip_address,
      user_ozone_id AS ozone_user_id,
      page_url,
      pixel_id,
      pixel_name
    FROM site_events
    WHERE internal_advertiser_id = '621'
),

-- first try to join by ozone_user_id
conversion_mapped AS (
    SELECT
      date_ts,
      ip_address,
      ozone_user_id,
      CASE 
        WHEN pixel_id = '10114' THEN 'GOSH_All_Site_Visitors'
        ELSE NULL
      END as conversion_All_Site_Visitors,
      CASE 
        WHEN pixel_id = '10116' THEN 'GOSH_Donation_Started'
        ELSE NULL
      END as conversion_Donation_Started,
      CASE 
        WHEN pixel_id = '10115' THEN 'GOSH_Successful_Donation'
        ELSE NULL
      END as conversion_Successful_Donation,
      page_url as page_url,
    FROM conversions_raw

),

-- apply regex to page_url

all_conversions AS (
SELECT 
date_ts,
ip_address,
ozone_user_id,
conversion_All_Site_Visitors,
conversion_Donation_Started,
conversion_Successful_Donation,
REGEXP_EXTRACT(page_url, r'utm_medium=([^&]+)') AS utm_medium,
REGEXP_EXTRACT(page_url, r'utm_source=([^&]+)') AS utm_source,
from conversion_mapped

),

-- Final aggregation to consolidate pixel data and merge duplicates
final_conversions AS (
  SELECT
    date_ts,
    ip_address,
    ozone_user_id,
    -- Aggregate all mapped pixel names into arrays
    ARRAY_AGG(DISTINCT conversion_All_Site_Visitors IGNORE NULLS) AS all_site_visitors_pixels,
    ARRAY_AGG(DISTINCT conversion_Donation_Started IGNORE NULLS) AS donation_started_pixels,
    ARRAY_AGG(DISTINCT conversion_Successful_Donation IGNORE NULLS) AS successful_donation_pixels,
    ARRAY_AGG(DISTINCT utm_medium IGNORE NULLS) AS utm_medium,
    ARRAY_AGG(DISTINCT utm_source IGNORE NULLS) AS utm_source,
  FROM all_conversions
  GROUP BY
    1,2,3
)

-- Aggregate final results
SELECT
  date_ts,
  
  -- Use COALESCE to handle NULLs for utm_medium and utm_source
  COALESCE(flattened_utm_medium, 'null') AS utm_medium,
  COALESCE(flattened_utm_source, 'direct') AS utm_source,

  -- conversion metrics
  SUM(ARRAY_LENGTH(all_site_visitors_pixels)) AS total_all_site_visitors,
  SUM(ARRAY_LENGTH(donation_started_pixels)) AS total_donation_started,
  SUM(ARRAY_LENGTH(successful_donation_pixels)) AS total_successful_donations,

FROM (
    SELECT 
      fc.*,
      
      -- Unnest utm_medium and utm_source only when they exist; otherwise, default to a single value
      CASE 
        WHEN utm_medium IS NULL THEN ['null']
        ELSE utm_medium
      END AS utm_medium_filled,

      CASE 
        WHEN utm_source IS NULL THEN ['direct']
        ELSE utm_source
      END AS utm_source_filled
    FROM final_conversions AS fc
) AS pre_flattened

-- LEFT JOIN on the unnested utm_medium and utm_source to preserve all rows
LEFT JOIN UNNEST(pre_flattened.utm_medium_filled) AS flattened_utm_medium
LEFT JOIN UNNEST(pre_flattened.utm_source_filled) AS flattened_utm_source

GROUP BY 
  date_ts, 
  flattened_utm_medium, 
  flattened_utm_source

ORDER BY date_ts DESC
