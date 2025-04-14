with fct_attribution__root_init as (
  select
    user_pwid_raw,
    browser_name,
    device_type,
    os_name,
    ip_address,
    is_household,
    user_id_ozone,

    coalesce(conversions.advertiser_domain, delivery.advertiser_domain) as advertiser_domain,
    coalesce(delivery.campaign_id, null) as campaign_id,
    coalesce(delivery.opportunity_name, null) as opportunity_name,
    coalesce(conversions.ts, delivery.ts) as ts, 
    if(delivery.ts is not null, 1, null) as impression,
    conversion,
    -- click, -- removing clicks, will use request_id / impression_id instead for attribution in separate stage
  from
    `ozone-analytics-dev.dbt_simon.fct_attribution__root`
  left join unnest(conversions) conversions
  left join unnest(delivery) delivery
),

get_impressions_conversions as ( -- apply primary key for impressions, conversions and output all impressions and all conversions (i.e. includes impressions even if no conversions)
  select 
    *, 
    case when impression > 0 then
      to_hex(
          md5(
            cast(
              coalesce(cast(user_pwid_raw as string), '') || '-' || 
              coalesce(cast(browser_name as string), '') || '-' || 
              coalesce(cast(device_type as string), '') || '-' || 
              coalesce(cast(os_name as string), '') || '-' || 
              coalesce(cast(ip_address as string), '') || '-' || 
              coalesce(cast(is_household as string), '') || '-' || 
              coalesce(cast(user_id_ozone as string), '') || '-' || 
              coalesce(cast(advertiser_domain as string), '') || '-' || 
              coalesce(cast(ts as string), '') || '-' || 
              coalesce(cast(impression as string), '')
            as string)
    )) end as impression_primary_key,
    case when conversion > 0 then
      to_hex(
          md5(
            cast(
              coalesce(cast(user_pwid_raw as string), '') || '-' || 
              coalesce(cast(advertiser_domain as string), '') || '-' || 
              coalesce(cast(ts as string), '') || '-' || 
              coalesce(cast(conversion as string), '')          
            as string)
    )) end as conversion_primary_key
  from fct_attribution__root_init
  where 
    -- user_pwid_raw is not null -- keeping all impressions regardless of pwid presence for downstream attribution flavours that don't use pwid
    advertiser_domain != ''
  qualify
    count(impression) over (partition by    
      user_pwid_raw,
      browser_name,
      device_type,
      os_name,
      ip_address,
      is_household,
      user_id_ozone, 
      advertiser_domain
    ) > 0 
    or count(conversion) over (partition by user_pwid_raw, advertiser_domain) > 0 -- conversions are using pwid for attribution
  order by 1,8,9
),

impressions as ( -- get only impressions
  select
    user_pwid_raw,
    browser_name,
    device_type,
    os_name,
    ip_address,
    is_household,
    user_id_ozone, 
    advertiser_domain,
    campaign_id,
    opportunity_name,
    ts,
    impression_primary_key
  from get_impressions_conversions
  where impression_primary_key is not null
),

conversions as ( -- get only conversions
  select
    user_pwid_raw,
    advertiser_domain,
    ts,
    conversion_primary_key
  from get_impressions_conversions
  where conversion_primary_key is not null  
),

map_all_events as ( -- map all conversions to impressions regardless of row duplications
  select 
    impressions.* except(ts, impression_primary_key),

    impressions.ts as impression_ts,
    impressions.impression_primary_key,
    -- timestamp_add(impressions.ts, interval 1 day) as mapping_window_day_ts, -- check conversion_ts is within impression_ts and this field so day attribution works 
    -- timestamp_add(impressions.ts, interval 7 day) as mapping_window_week_ts, -- check conversion_ts is within impression_ts and this field so week attribution works 
    -- conversions
    conversions.ts as conversion_ts,
    conversion_primary_key,
    case when conversions.ts between impressions.ts and timestamp_add(impressions.ts, interval 1 day) then true end as is_conversion_attribution_day, -- flag for conversion occuring wihtin attribution window of one day
    case when conversions.ts between impressions.ts and timestamp_add(impressions.ts, interval 7 day) then true end as is_conversion_attribution_week, -- flag for conversion occuring wihtin attribution window of one week
  from impressions
  left join conversions 
    on impressions.user_pwid_raw = conversions.user_pwid_raw
      and impressions.advertiser_domain = conversions.advertiser_domain
),

find_dedupe_mapped_events_1 as ( -- apply distinct row_num value to mapped events to filter for in next step
  select 
    *,

    case when is_conversion_attribution_day is true then 
      row_number() over(partition by user_pwid_raw, advertiser_domain, conversion_primary_key, is_conversion_attribution_day order by impression_ts) 
    end as conversion_attr_day_rownum,
    case when is_conversion_attribution_week is true then 
      row_number() over(partition by user_pwid_raw, advertiser_domain, conversion_primary_key, is_conversion_attribution_week order by impression_ts) 
    end as conversion_attr_week_rownum,
  from map_all_events
),

find_dedupe_mapped_events_2 as ( -- keeps all distinct impressions, but flag distinct conversion event occurrences (where ...rownum value is 1), for correct aggregation
  select 
    *,

    case when conversion_attr_day_rownum = 1 then conversion_primary_key end as mapped_conversion_day_primary_key,
    case when conversion_attr_week_rownum = 1 then conversion_primary_key end as mapped_conversion_week_primary_key,
  from find_dedupe_mapped_events_1
),

final as (
   select 
     --user_pwid_raw,
     browser_name,
     device_type,
     --os_name,
     --ip_address,
     --is_household,
     --user_id_ozone,
     advertiser_domain,
     campaign_id,
    opportunity_name,
    date(impression_ts) as impression_date,
    count(distinct impression_primary_key) as impressions,
    nullif(count(distinct mapped_conversion_day_primary_key), 0) as conversions_attributed_day,
   nullif(count(distinct mapped_conversion_week_primary_key), 0) as conversions_attributed_week,
 from find_dedupe_mapped_events_2
 group by all
 )

 SELECT * FROM final

-- select * from find_dedupe_mapped_events_2

-- where user_pwid_raw = '1_82.46.209.35Apple iPhoneiOSMobile Safari' -- test pwid