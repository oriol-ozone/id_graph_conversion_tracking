with conversion_mapping as (
  select * from `ozone-analytics-dev.dbt_zac.fct_attribution_conversion_mapping_v6`
)

select 
  date,
  campaign_id,
  sf_line_id,   
  opportunity_name,
  io_number,    
  browser_name,
  count(distinct impression_primary_key) as impressions,
  -- count beeswax clicks impressions is redundant, only 1 beeswax_click event per impression
  count(distinct if(is_beeswax_click is true, impression_primary_key, null)) as beeswax_clicks, 
  -- sum used as tracker clicks can be > 1 per impression
  sum(if(impression_row_num = 1, tracker_click, null)) as tracker_clicks,
   -- count distinct impressions with at least 1 tracker click
  count(distinct if(is_tracker_click is true, impression_primary_key, null)) as tracker_click_impressions,
  count(distinct conversion_primary_key) as conversions,
  -- count conversions based on different windows
  count(DISTINCT IF(days_since_impression <= 7, conversion_primary_key, null)) as `7D_pv_conversions`,
  count(DISTINCT IF(days_since_impression <= 30, conversion_primary_key, null)) as `30D_pv_conversions`,
  count(DISTINCT IF(days_since_impression <= 7 AND is_tracker_click is true, conversion_primary_key, null)) as `7D_pc_conversions`,
  count(DISTINCT IF(days_since_impression <= 30 AND is_tracker_click is true, conversion_primary_key, null)) as `30D_pc_conversions`,
  -- count distinct beeswax click conversions
  count(distinct if(is_beeswax_click_conversion is true, conversion_primary_key, null)) as beeswax_click_conversions,
  -- count distinct tracker click conversions
  count(distinct if(is_tracker_click_conversion is true, conversion_primary_key, null)) as tracker_click_conversions,
  sum(if(impression_row_num = 1, bid_gross_usd, null)) / 1000 as bid_gross_usd,
from conversion_mapping
group by all 
order by 1