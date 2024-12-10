WITH site_events AS (
    SELECT *
    FROM `ozone-analytics-dev.ozone.stg_ozone__site_events_1_0_1`
    WHERE 
        ts BETWEEN TIMESTAMP("2024-12-09T18:00:00") AND TIMESTAMP("2024-12-09T19:00:00")
        --DATE(ts) = CURRENT_DATE - 1
),

conversions as (
    SELECT 
    DATE(ts) AS date_ts,
    ip_address,
    browser_name,
    device_type,
    os_name,
    os_version,
    user_ozone_id as ozone_user_id,
    pixel_name
    FROM site_events
    WHERE internal_advertiser_id = '621'

)

SELECT 
date_ts,
pixel_name,
COUNT(DISTINCT ozone_user_id) as distinct_ozone_user_id_count,
COUNT(ozone_user_id) as total_ozone_user_id_count,
COUNT(DISTINCT CONCAT(ip_address,browser_name,device_type,os_name,os_version)) as distinct_pwid_count,
COUNT(CONCAT(ip_address,browser_name,device_type,os_name,os_version)) as total_pwid_count
FROM conversions
group by all