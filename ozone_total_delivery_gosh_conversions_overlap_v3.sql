WITH date_interval AS (
    -- Define the base interval for dim_bidstream
    SELECT 
        DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AS dim_start_date,
        DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AS site_events_start_date,
        DATE_SUB(CURRENT_DATE(), INTERVAL 23 DAY) AS site_events_end_date,
),

dim_bidstream AS (
    SELECT *
    FROM ozone.dim_bidstream, date_interval
    WHERE date(ts) BETWEEN date_interval.dim_start_date AND date_interval.dim_start_date
    --DATE(ts) = CURRENT_DATE - 1
    AND user_country = 'United Kingdom'
),
-- pull impressions and spend by IP and ozone id for the GOSH campaign

ad_level_raw AS (
    SELECT
      DATE(ts) AS date_ts,
      request_id,
      ad_id,
      ip_address,
      ozone_user.user_id AS ozone_user_id,
      ad.is_impression as is_impression,
      (SELECT MAX(max_bid_net_usd) FROM UNNEST(ad.seat) AS seat WHERE seat.max_bid_net_usd IS NOT NULL AND seat.is_impression IS TRUE) AS max_bid_net_usd
      FROM dim_bidstream
      LEFT JOIN UNNEST(ad) AS ad
),

-- bring in hhip table for eu

hhip_eu AS (
  SELECT 
    DISTINCT
    mapping.date AS date_h,
    mapping.ip_address_prefix
  FROM `ozone-analytics-dev.ozone.fct_ids__ip_address_prefix_mapping` AS mapping,
       date_interval
  LEFT JOIN UNNEST(mapping.user_ad_platform_ids) AS user_ad_platform_ids
  WHERE 
    mapping.date = date_interval.dim_start_date -- Use dim_start_date from date_interval
    AND mapping.app_region = 'eu'
    AND mapping.order_tier <= 55000000
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

-- first add the is_household flag

is_household_table AS (
  SELECT 
    date_ts,
    request_id,
    ad_id,
    ozone_user_id,
    ip_address,
    is_impression,
    max_bid_net_usd,
    CASE 
      WHEN hhip_eu.ip_address_prefix IS NULL then FALSE
      ELSE TRUE
    END as is_household
    FROM ad_level_raw
    LEFT JOIN hhip_eu ON hhip_eu.ip_address_prefix = ip_address AND hhip_eu.date_h = date_ts
),

ad_data_aggregated AS (
    SELECT
        date_ts,
        ozone_user_id,
        ip_address,
        is_household,
        
        -- Aggregate ad-level metrics
        COUNT(request_id) AS total_requests,
        COUNTIF(is_impression) AS total_impressions,
        ROUND(SUM(SAFE_DIVIDE(max_bid_net_usd, 1000)), 2) AS total_revenue,
        
        -- Calculate CPM 
        ROUND(SUM(SAFE_DIVIDE(max_bid_net_usd, 1000)) / NULLIF(COUNTIF(is_impression), 0) * 1000, 2) AS avg_cpm
        
    FROM is_household_table
    GROUP BY
        date_ts,
        ozone_user_id,
        ip_address,
        is_household
),

-- aggregate conversion data at ozone_user_id and ip_address separately 

conversion_ozone_id_agg AS (
    SELECT 
    date_ts,
    ozone_user_id,
    REGEXP_EXTRACT(page_url, r'utm_medium=([^&]+)') AS utm_medium,
    REGEXP_EXTRACT(page_url, r'utm_source=([^&]+)') AS utm_source,
    SUM(CASE WHEN pixel_id = '10114' THEN 1 ELSE 0 END) as GOSH_All_Site_Visitors,
    SUM(CASE WHEN pixel_id = '10116' THEN 1 ELSE 0 END) as GOSH_Donation_Started,
    SUM(CASE WHEN pixel_id = '10115' THEN 1 ELSE 0 END) as GOSH_Successful_Donation
    FROM conversions_raw
    GROUP BY 1, 2, 3, 4
)
,

conversion_ip_address_agg AS (
    SELECT 
    date_ts,
    ip_address,
    REGEXP_EXTRACT(page_url, r'utm_medium=([^&]+)') AS utm_medium,
    REGEXP_EXTRACT(page_url, r'utm_source=([^&]+)') AS utm_source,
    SUM(CASE WHEN pixel_id = '10114' THEN 1 ELSE 0 END) as GOSH_All_Site_Visitors,
    SUM(CASE WHEN pixel_id = '10116' THEN 1 ELSE 0 END) as GOSH_Donation_Started,
    SUM(CASE WHEN pixel_id = '10115' THEN 1 ELSE 0 END) as GOSH_Successful_Donation
    FROM conversions_raw
    GROUP BY 1, 2, 3, 4
),


joined_by_user_id AS (
    SELECT
        ad.date_ts as date_ts,
        ad.ozone_user_id as ozone_user_id,
        ad.ip_address as ip_address,
        is_household,
        
        -- Ad-level aggregated metrics
        ad.total_requests as total_requests,
        ad.total_impressions as total_impressions,
        ad.total_revenue as total_revenue,
        ad.avg_cpm as avg_cpm,
        
        -- Conversion Mapping
        c.GOSH_All_Site_Visitors as GOSH_All_Site_Visitors,
        c.GOSH_Donation_Started as GOSH_Donation_Started,
        c.GOSH_Successful_Donation as GOSH_Successful_Donation,
        
        c.utm_medium AS utm_medium,
        c.utm_source AS utm_source,
        CASE WHEN GOSH_All_Site_Visitors is not null then 'ozone_id' else null END AS conversion_mapping_source
        
    FROM ad_data_aggregated ad
    LEFT JOIN conversion_ozone_id_agg c 
        ON ad.ozone_user_id = c.ozone_user_id
),

joined_by_hhip AS (
    SELECT
        juid.date_ts as date_ts,
        juid.ozone_user_id as ozone_user_id,
        juid.ip_address as ip_address,
        is_household,
        
        -- Ad-level aggregated metrics
        juid.total_requests as total_requests,
        juid.total_impressions as total_impressions,
        juid.total_revenue as total_revenue,
        juid.avg_cpm as avg_cpm,
        
        -- Conversion Mapping
        c.GOSH_All_Site_Visitors as GOSH_All_Site_Visitors,
        c.GOSH_Donation_Started as GOSH_Donation_Started,
        c.GOSH_Successful_Donation as GOSH_Successful_Donation,
        
        c.utm_medium AS utm_medium,
        c.utm_source AS utm_source,
        CASE WHEN c.GOSH_All_Site_Visitors is not null then 'hhip' else null END AS conversion_mapping_source
        
    FROM (SELECT * FROM joined_by_user_id WHERE GOSH_All_Site_Visitors is null) juid -- attempt to map only the instances where we haven't mapped conversions in the previous step
    LEFT JOIN conversion_ip_address_agg c 
        ON juid.ip_address = c.ip_address
),


all_conversions AS (
    SELECT 
        date_ts,
        ozone_user_id,
        ip_address,
        is_household,
        
        -- Ad-level aggregated metrics
        total_requests,
        total_impressions,
        total_revenue,
        avg_cpm,
        
        -- Conversion Mapping
        GOSH_All_Site_Visitors,
        GOSH_Donation_Started,
        GOSH_Successful_Donation,
        
        -- Extract UTM Parameters
        utm_medium,
        utm_source,
        
        conversion_mapping_source
        
    FROM joined_by_user_id 
    WHERE GOSH_All_Site_Visitors is not null -- only include the rows mapped using ozone id 

    UNION ALL

    SELECT 
        date_ts,
        ozone_user_id,
        ip_address,
        is_household,
        
        -- Ad-level aggregated metrics
        total_requests,
        total_impressions,
        total_revenue,
        avg_cpm,

        -- Conversion Mapping
        GOSH_All_Site_Visitors,
        GOSH_Donation_Started,
        GOSH_Successful_Donation,
        
        -- Extract UTM Parameters
        utm_medium,
        utm_source,
        
        conversion_mapping_source
        
    FROM joined_by_hhip -- including all the rest of rows, mapped and non-mapped in the hhip lookup query
),

-- remove ozoneid and hhip breakdown and group the two unioned queries

final_conversions AS (
    SELECT
        date_ts,
        is_household,
        conversion_mapping_source,
        
        -- Aggregate ad-level metrics
        SUM(total_requests) AS total_requests,
        SUM(total_impressions) AS total_impressions,
        SUM(total_revenue) AS total_revenue,
        
        -- Aggregate conversion data into arrays
        SUM(GOSH_All_Site_Visitors) as GOSH_All_Site_Visitors,
        SUM(GOSH_Donation_Started) as GOSH_Donation_Started,
        SUM(GOSH_Successful_Donation) as GOSH_Successful_Donation,

        
        -- Aggregate UTM parameters
        utm_medium,
        utm_source
        
    FROM all_conversions
    GROUP BY
        date_ts,
        is_household,
        conversion_mapping_source,
        utm_medium,
        utm_source
)

SELECT * FROM final_conversions

