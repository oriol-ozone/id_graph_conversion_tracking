
with fct_attribution__root_init as (
  select
    user_pwid_raw,
    browser_name,
    device_type,
    os_name,
    ip_address,
    is_household,
    user_id_ozone,

    coalesce(conversions.ts, delivery.ts) as ts, 
    coalesce(conversions.advertiser_domain, delivery.advertiser_domain, delivery.tracker_advertiser_domain) as advertiser_domain, -- multiple ad domains possible in non-beeswax logs, to match with site_sessions, try site_sessions.advertiser_domain in unnest(split(dim_pages.advertiser_domain))
    coalesce(delivery.request_id, null) as request_id,
    coalesce(delivery.ad_id, null) as ad_id,
    coalesce(delivery.sf_line_id, null) as sf_line_id,
    coalesce(delivery.advertiser_id, null) as advertiser_id,
    coalesce(delivery.campaign_id, null) as campaign_id,
    coalesce(delivery.creative_id, null) as creative_id,
    coalesce(delivery.io_number, null) as io_number,
    coalesce(delivery.opportunity_name, null) as opportunity_name,
    coalesce(delivery.bid_gross_usd, null) as bid_gross_usd,
    if(delivery.ts is not null, 1, null) as impression,
    delivery.beeswax_clicks,
    delivery.tracker_click,
    coalesce(conversions.pixel_name, null) as pixel_name, 
    coalesce(conversions.pixel_id, null) as pixel_id, 
    conversion,
    
  from
    `ozone-analytics-dev.dbt_zac.fct_attribution__root_v3` -- data between '2025-04-23' and '2025-04-29'
    -- `ozone-analytics-dev.dbt_zac.PI_234_fct_attribution__root_view_v3` -- rolling 7 days from current date - 1
  left join unnest(conversions) conversions
  left join unnest(delivery) delivery
),

get_impressions_conversions as ( -- apply primary key to distinct conversions, output all impressions, and all conversion occurrences by partition 
  select 
    fct_attribution__root_init.*,

    if(conversion > 0,
      row_number() over(partition by 
        user_pwid_raw,
        ts,
        advertiser_domain,
        pixel_name, 
        pixel_id, 
        conversion
        order by ts),
      null
    ) as conversion_row_num, -- only used to generate conversion_primary_key, treats all conversion events as distinct (even if same pixel, ts, pwid etc.)
  from fct_attribution__root_init
  qualify -- keeps some rows where no impression / conversion, but at least 1 or more within same partition group did
    count(impression) over (partition by  
      -- ts, -- in context of impression, should this be changed to when an impression occurred instead of impression count by grouped user / device?
      -- ad_id,
      -- creative_id  
      user_pwid_raw,
      browser_name,
      device_type,
      os_name,
      ip_address,
      is_household,
      user_id_ozone, 
      advertiser_domain
    ) > 0 
    or count(conversion) over (partition by user_pwid_raw, advertiser_domain) > 0
),

get_impressions as ( -- get only impressions
  select
    user_pwid_raw,
    browser_name,
    device_type,
    os_name,
    ip_address,
    is_household,
    user_id_ozone, 
    advertiser_domain,
    ts as impression_ts,
    campaign_id,
    opportunity_name,
    ad_id,
    sf_line_id,
    creative_id,
    io_number,
    bid_gross_usd,
    beeswax_clicks,
    tracker_click,

    to_hex(
        md5(
          cast(
            coalesce(cast(ts as string), '') || '-' || -- apply same impression occurred only logic as suggested in qualify clause prev cte?
            coalesce(cast(ad_id as string), '') || '-' || 
            coalesce(cast(creative_id as string), '')   
            -- coalesce(cast(user_pwid_raw as string), '') || '-' || 
            -- coalesce(cast(ad_id as string), '') || '-' || 
            -- coalesce(cast(ts as string), '')          
          as string)
    )) as impression_primary_key -- not for impression count, for joining to map_conversions and identify duplicate impression rows
  from get_impressions_conversions
  where ad_id is not null
),

get_conversions as ( -- get conversions / clicks
  select
    user_pwid_raw,
    advertiser_domain,
    ts as conversion_ts,
    pixel_name,
    pixel_id,

    if( conversion_row_num > 0,
      to_hex(
          md5(
            cast(
              coalesce(cast(user_pwid_raw as string), '') || '-' || 
              coalesce(cast(advertiser_domain as string), '') || '-' || 
              coalesce(cast(ts as string), '') || '-' || 
              coalesce(cast(pixel_name as string), '') || '-' || 
              coalesce(cast(pixel_id as string), '') || '-' || 
              coalesce(cast(conversion_row_num as string), '')          
            as string)
      )),
      null
    ) as conversion_primary_key, 
  from get_impressions_conversions
  where conversion is not null 
),

map_conversions as ( -- map all conversions (incl duplicates) that occurred within 30 days since impression, get # hrs/days since impression
  select 
    get_impressions.*,
    conversion_ts,   
    pixel_name,
    pixel_id, 
    conversion_primary_key,
    
    if(datetime_diff(conversion_ts, impression_ts, hour) = 0, 1, datetime_diff(conversion_ts, impression_ts, hour)) as hrs_since_impression,
    row_number() over(partition by conversion_primary_key order by impression_ts, conversion_ts) as map_conversion_row_num, -- identify distinct conversions
  from get_impressions
  inner join get_conversions 
    on get_impressions.user_pwid_raw = get_conversions.user_pwid_raw
      and get_impressions.advertiser_domain = get_conversions.advertiser_domain
      and conversion_ts between impression_ts and timestamp_add(impression_ts, interval 30 day)
),

final as (
  select  
    date(get_impressions.impression_ts) as date,
    get_impressions.user_pwid_raw,
    get_impressions.browser_name,
    get_impressions.device_type,
    get_impressions.os_name,
    get_impressions.ip_address,
    get_impressions.user_id_ozone,

    get_impressions.campaign_id,
    get_impressions.sf_line_id,   
    get_impressions.opportunity_name,
    get_impressions.io_number,

    get_impressions.advertiser_domain,
    get_impressions.ad_id,
    get_impressions.creative_id,    
    get_impressions.impression_ts,
    get_impressions.impression_primary_key,

    get_impressions.bid_gross_usd, 
    get_impressions.beeswax_clicks,
    get_impressions.tracker_click,
    
    map_conversions.conversion_ts,
    map_conversions.pixel_name,
    map_conversions.pixel_id,
    map_conversions.conversion_primary_key,
    map_conversions.hrs_since_impression,

    if( map_conversions.conversion_ts is not null, 
      cast( ceiling( map_conversions.hrs_since_impression / 24 ) as int),
      null
    ) as days_since_impression, -- to get days, when (n hrs / 24) has remainder, round up to next full int (n of days) i.e 26 hrs becomes 2 days
    row_number() over(partition by date(get_impressions.impression_ts), get_impressions.ad_id, get_impressions.creative_id order by get_impressions.impression_ts) as impression_row_num, -- identify distinct impressions
    if(get_impressions.beeswax_clicks > 0, true, null) as is_beeswax_click, -- flag impressions with beeswax click
    if(get_impressions.tracker_click > 0, true, null) as is_tracker_click, -- flag impressions with tracker click
    if(map_conversions.conversion_primary_key is not null, true, null) as is_conversion_mapped, -- flag impressions with conversion mapped
    case 
      when get_impressions.beeswax_clicks > 0 and map_conversions.conversion_primary_key is not null then true 
    end as is_beeswax_click_conversion, -- flag beeswax click conversion
    case 
      when get_impressions.tracker_click > 0 and map_conversions.conversion_primary_key is not null then true 
    end as is_tracker_click_conversion, -- flag tracker click conversion
  from get_impressions
  left join map_conversions
    using(    
      impression_primary_key)
)

select * from final