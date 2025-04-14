WITH pixel_data as (   
    SELECT 
    sp.internal_advertiser_id,
    sp.pixel_id,
    sp.pixel_name

    FROM `ozone-analytics-dev.ozone.dim_site_sessions`,
    UNNEST(site_pages) as sp

    WHERE date = current_date - 16
    and sp.pixel_type = 'conversion'

),

winlog_data as (
    SELECT 
    advertiser_id,
    campaign_alt_id,
    campaign_id,
    line_item_alt_id

    FROM `ozone-analytics-dev.ozone.stg_beeswax__win_log` TABLESAMPLE SYSTEM (1 PERCENT)

    WHERE DATE(bid_time_utc) = current_date - 16

),

salesforce as (
    SELECT 
        Line_Item_Line_ID,
        IO_Number,
        Product,
        Line_Item_Record_Type,
        Line_ID_Start_Date,
        Line_ID_End_Date,
        Metric,
        Opportunity_Name,
        Account_ID,
        Account_Name,
        Industry,
        Agency,
        Customer_Group
    FROM `ozone-analytics-dev.salesforce.line_item_metadata`
),

final as (
    SELECT 
    internal_advertiser_id,
    pixel_id,
    pixel_name,
    -- winlog fields to check
    advertiser_id,
    line_item_alt_id,
    campaign_alt_id,
    campaign_id,
    -- salesforce fields
    Line_Item_Line_ID as salesforce_LI_ID,
    IO_Number,
    Product,
    Line_Item_Record_Type,
    min(Line_ID_Start_Date) as Line_ID_Start_Date,
    max(Line_ID_End_Date) as Line_ID_End_Date,
    Metric,
    Opportunity_Name,
    Account_ID,
    Account_Name,
    Industry,
    Agency,
    Customer_Group,
    count(*) number_pixel_events

    FROM pixel_data
    LEFT JOIN winlog_data ON advertiser_id = SAFE_CAST(internal_advertiser_id AS INT64)
    LEFT JOIN salesforce ON line_item_alt_id = Line_Item_Line_ID

    GROUP BY ALL

)

SELECT * FROM final 
