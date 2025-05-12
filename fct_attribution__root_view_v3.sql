/* created on: 29 april 2025
  author: zac c
  description: reconfiguring PI_234_fct_attribution__root_view_v2 to use dim_pages newly added impression array level fields (request_id, bid_creative_id, and ts)
  purpose is to address two issues:
  - rare case of ad_ids reused across different campaign ids leading to user/device impression mismatch
  - allow post click attribution for conversions
*/

#create or replace table ozone-analytics-dev.dbt_zac.fct_attribution__root_v3 as

with fct_ip_address_prefix as (
  select
    date,
    app_region,
    ip_address_prefix,
    is_household
  from `ozone-analytics-dev`.`ozone`.`fct_ip_address_prefix`
  -- where date = '2025-04-29'  -- used to generate table fct_attribution__root_v3
  where date = current_date - 1
),

dim_pages as (
  select
    dim_pages_primary_key,

    dim_pages.ts as dim_pages_ts, -- dim_pages min(ts) field
    date(dim_pages.ts) as date, -- dim_pages date based on min(ts) field
    ip_address,
    is_household,
    ad_id,
    advertiser_domain,
    ozone_user.user_pwid_raw,
    ozone_user.user_id as user_id_ozone,
    browser_name,
    device_type,
    os_name,
    bid_net_usd,
    bid_gross_usd,

    impression.request_id, -- impression level request_id 
    ltrim(impression.bid_creative_id, 'ozone-id') as creative_id, -- impression level bid_creative_id
    impression.ts as ts, -- impression level ts for each impression
  from
    `ozone-analytics-dev.ozone.dim_pages` dim_pages
    left join fct_ip_address_prefix 
      on ip_address = ip_address_prefix
      and dim_pages.app_region = fct_ip_address_prefix.app_region
    left join unnest(impression) as impression
  where
    -- date(dim_pages.ts) between '2025-04-23' and '2025-04-29' -- used to generate table fct_attribution__root_v3
    date(dim_pages.ts) between current_date - 7 and current_date - 1
    and advertiser_domain is not null 
    and seat_name like '%beeswax%'
),

win_log_salesforce as (   
  select 
    date(bid_time_utc) as date,
    exchange_imp_id as ad_id, 
    sf_line_id,
    advertiser_id,
    campaign_id,
    creative_id,
    io_number, 
    opportunity_name,
    bid_time_utc as ts,

    exchange_auction_id as request_id,
    nullif(clicks, 0) as beeswax_clicks,
    if(in_view_time_millis > 0, in_view_time_millis, null) as beeswax_in_view_time
  from `ozone-analytics-dev`.`ozone`.`stg_beeswax__win_log` 
  left join `ozone-analytics-dev`.`ozone`.`stg_salesforce__line_item_metadata`
    on stg_beeswax__win_log.line_item_alt_id = stg_salesforce__line_item_metadata.sf_line_id
  where 
    -- date(bid_time_utc) between '2025-04-23' and '2025-04-29' -- used to generate table fct_attribution__root_v3
    date(bid_time_utc) between current_date - 7 and current_date - 1
  -- qualify row_number() over (partition by date, ad_id, creative_id, request_id) = 1 -- replaced by adding creative_id in inner join dedupe
),

site_sessions_tracker as (
  select
    date(site_pages.ts) as date,
    split(var1, '|')[safe_offset(0)] as request_id,
    split(var1, '|')[safe_offset(1)] as ad_id,
    rtrim(replace (replace (replace (advertiser_domain, 'http://', ''), 'https://', ''), 'www.', ''), '/') as tracker_advertiser_domain,
    ltrim(split(var2, '|')[safe_offset(3)], 'ozone-')  as creative_id,
    count(if(pixel_name = 'clickTrackingReporter-adClicked', 1, null)) as tracker_click,
    count(if(pixel_name = 'clickTrackingReporter-activeHover', 1, null)) as tracker_hovers, 
  from
    `ozone-analytics-dev.ozone.dim_site_sessions` dim_site_sessions
    left join unnest(site_pages) site_pages
  where
    -- date between '2025-04-23' and '2025-04-29' -- used to generate table fct_attribution__root_v3
    date between current_date - 7 and current_date - 1
    and pixel_type = 'tracker'
  group by 1,2,3,4,5
  having 
    tracker_click > 0 
    or tracker_hovers > 0
),

stg_dv__attention_metrics as (
  select
    date,
    oz_imp_id as ad_id,
    dv_net_80_viewable_imp,
    dv_net_100_viewable_imp,
    dv_net_iab_viewable_imp,
    dv_ad_mouse_hover_imp,
  from
    `ozpr-data-engineering-prod.prod_de_stg.stg_dv__attention_metrics`
  where
    -- date between '2025-04-23' and '2025-04-29' -- used to generate table fct_attribution__root_v3
    date between current_date - 7 and current_date - 1
),

dim_pages_init as (
  select 
    dim_pages_primary_key,
    
    user_pwid_raw,
    ip_address,
    is_household,
    user_id_ozone,
    advertiser_domain,
    browser_name,
    device_type,
    os_name,
    bid_net_usd,
    bid_gross_usd,
    ad_id,
    dim_pages.request_id as impression_request_id,
    dim_pages.creative_id as impression_creative_id,

    win_log_salesforce.ts,
    sf_line_id,
    advertiser_id,
    campaign_id,
    win_log_salesforce.creative_id,
    win_log_salesforce.request_id,
    io_number,
    opportunity_name,
    beeswax_clicks,
    beeswax_in_view_time,

    tracker_advertiser_domain,
    tracker_click,
    tracker_hovers,

    dv_net_80_viewable_imp,
    dv_net_100_viewable_imp,
    dv_net_iab_viewable_imp,
    dv_ad_mouse_hover_imp,
  from dim_pages
  inner join win_log_salesforce using(date, ad_id, creative_id)
  left join site_sessions_tracker using(date, ad_id, creative_id) # should this include request_id?
  left join stg_dv__attention_metrics using(date, ad_id)
),

dim_pages_agg as (
  select 
    date(ts) as date,
    user_pwid_raw,
    ip_address,
    is_household,
    user_id_ozone,
    browser_name,
    device_type,
    os_name,
    array_agg((
      select as struct
        ts,
        request_id,
        advertiser_domain,
        ad_id,
        sf_line_id,
        advertiser_id,
        campaign_id,
        creative_id,
        io_number,
        opportunity_name,
        bid_net_usd,
        bid_gross_usd,
        beeswax_clicks,
        beeswax_in_view_time,
        tracker_advertiser_domain,
        tracker_click,
        tracker_hovers,
        dv_net_80_viewable_imp,
        dv_net_100_viewable_imp,
        dv_net_iab_viewable_imp,
        dv_ad_mouse_hover_imp,
    )) delivery
  from dim_pages_init
  group by 1,2,3,4,5,6,7,8
),

site_sessions_conversions_init as (
  select
    date(site_pages.ts) as date,
    user_pwid_raw,
    ip_address,
    is_household,
    user_id_ozone,
    case
      when browser_name in ('Chrome','Safari','UIWebView','Chrome Webview','SamsungBrowser','Firefox','Silk','Edge','Flipboard','Internet Explorer') then browser_name
      when browser_name in ('Mobile Chrome') then 'Chrome'
      when browser_name in ('Chrome WebView') then 'Chrome Webview'
      when browser_name in ('Mobile Safari', 'safari') then 'Safari'
      when browser_name in ('Facebook', 'GSA', 'WebKit') then 'UIWebView'
      when browser_name in ('Mobile Firefox') then 'Firefox'
      when browser_name in ('Samsung Internet') then 'SamsungBrowser'
      else 'other'
    end browser_name,
    device_type,
    os_name,
    site_pages.ts,
    rtrim(replace (replace (replace (advertiser_domain, 'http://', ''), 'https://', ''), 'www.', ''), '/') as advertiser_domain,
    pixel_id,
    pixel_name,
    if(pixel_type = 'conversion', 1, null) as conversion,
  from
    `ozone-analytics-dev.ozone.dim_site_sessions` dim_site_sessions
    left join unnest(site_pages) site_pages
  where
    -- date between '2025-04-23' and '2025-04-29' -- used to generate table fct_attribution__root_v3
    date between current_date - 7 and current_date - 1
    and pixel_type = 'conversion'
),

site_sessions_conversions_agg as (
  select
    date,
    user_pwid_raw,
    ip_address,
    is_household,
    user_id_ozone,
    browser_name,
    device_type,
    os_name,
    array_agg((
      select as struct 
        ts,
        advertiser_domain,
        pixel_id,
        pixel_name,
        conversion
    )) conversions
  from site_sessions_conversions_init
  where 
    conversion = 1 
  group by 1,2,3,4,5,6,7,8
)


select 
  date,
  user_pwid_raw,
  browser_name,
  device_type,
  os_name,
  ip_address,
  is_household,
  user_id_ozone,
  null as conversions,
  delivery
from dim_pages_agg

union all

select 
  date,
  user_pwid_raw,
  browser_name,
  device_type,
  os_name,
  ip_address,
  is_household,
  user_id_ozone,
  conversions,
  null as delivery
from site_sessions_conversions_agg

## dim_pages / win_log_salesforce ad_id and request_id match but not creative_ids, dates from 2025-04-25 to 2025-04-27
-- select 
--   dim_pages_primary_key,
--   date as dim_pages_date,
--   ad_id as dim_pages_ad_id,
--   dim_pages.request_id as dim_pages_request_id,
--   dim_pages.creative_id as dim_pages_creative_id,

--   win_log_salesforce.date,
--   win_log_salesforce.ad_id,
--   win_log_salesforce.creative_id,
--   win_log_salesforce.request_id
-- from dim_pages 
--   left join win_log_salesforce using(date, ad_id) -- , creative_id, request_id)
-- where dim_pages_primary_key in (
--   'd9dacf990aeb311d466a46a2142016a3',
--   '7674fc853005efbbaf1e35dee469cf0d',
--   '81157147242cc556179fd748dc99a732',
--   '92642b3a3a34b977e14f126a61cd9970'
--   )
--   or ad_id in (
--   '287d78ab2f40c8e48',
--   '295f4dd14f25f1d1',
--   '34d9750d18db43c',
--   '32238d3043f7ba9c'   
--   )