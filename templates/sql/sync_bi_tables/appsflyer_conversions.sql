/*
    * Supplements appsflyer.all_installs with information on conversion to free trial and paid.

    * Each row is an appsflyer first installation that is also in calm devices.

    * Conversions are attributed to the most recent appsflyer device seen before the conversion.

    * If an account converts on a different device than their appsflyer installation device,
      they can still be attributed to that appsflyer install if both devices are associated
      with the same user id.
*/
begin;
drop table if exists {{ params.schema }}.appsflyer_conversions_stage;
create table {{ params.schema }}.appsflyer_conversions_stage
distkey (calm_device_id)
interleaved sortkey (install_time,
                     platform,
                     media_source)
as
-- clean advertising ids from appdb.devices
with get_appdb_devices as (
    select id,
           platform,
           first_seen,
           "type",
           case when advertising_id is not null
                and upper(advertising_id) <> upper(id)
                then advertising_id
           end as devices_advertising_id
    from appdb.devices
    where platform in ('ios', 'android')
),
-- clean advertising ids from backfill tables and combine with appdb.devices
get_devices as (
    select *
    from (
              select *,
                     upper(case platform when 'ios'
                                         then id
                                         when 'android'
                                         then advertising_id
                     end) as device_id_to_match,
                     row_number() over (partition by device_id_to_match order by first_seen) as device_identifier_num
              from (
                       select platform,
                              id,
                              first_seen,
                              "type",
                              devices_advertising_id as advertising_id
                       from get_appdb_devices

                       union all

                       select d.platform,
                              d.id,
                              d.first_seen,
                              d."type",
                              upper(coalesce(b.google_ad_id, i.idfa)) as advertising_id
                       from get_appdb_devices d
                       left join data.google_ad_id_map b
                            on d.platform = 'android'
                            and b.device_id = d.id
                       left join data.idfa_map i
                            on d.platform = 'ios'
                            and i.device_id = d.id
                   )
         )
    where device_identifier_num = 1
),
-- find first matching device id in appsflyer
get_appsflyer_installs as (
    select *
    from (
            select *,
                   upper(case platform when 'ios'
                                       then idfv
                                       when 'android'
                                       then idfa
                   end) as device_id_to_match,
                   -- if multiple, only use first
                   row_number() over (partition by device_id_to_match order by install_time) as install_num
              from appsflyer.all_installs
              where platform in ('ios', 'android')
         )
    where install_num = 1 -- first install must fit criteria
),
-- find first trial per device
-- (start join with subscriptions rather than devices because acnt_id distkey on device_acnts, not device_id)
get_free_trial_devices as (
    select s.*,
           coalesce(s.device_id, da.device_id) as sub_linked_device_id
    from (
              select *,
                     row_number() over (partition by acnt_id order by purchased) as free_trial_num
              from {{ params.schema }}.subscriptions_stage
              where plan_is_free = true
              and is_trial_period = true
         ) s
    left join {{ params.schema }}.device_acnts_stage da on da.acnt_id = s.acnt_id
    where s.free_trial_num = 1
),
-- find first payment per device
-- (start join with subscriptions rather than devices because acnt_id distkey on device_acnts, not device_id)
get_paid_devices as (
    select s.*,
           coalesce(s.device_id, da.device_id) as sub_linked_device_id
    from (
              select *,
                     row_number() over (partition by acnt_id order by purchased) as payment_num
              from {{ params.schema }}.subscriptions_stage
              where plan_is_free = false
         ) s
    left join {{ params.schema }}.device_acnts_stage da on da.acnt_id = s.acnt_id
    where s.payment_num = 1
),
-- join appsflyer installs with first free trial and first payment info
get_appsflyer_subscriptions as (
   select *
   from (
             select af.*,
                    split_part(af.device_type, '-', 1) as device_make,
                    split_part(af.device_type, '-', 2) as device_model_name,
                    split_part(af.device_type, '-', 3) as device_model_num,
                    af.device_id_to_match as appsflyer_device_id,
                    d.id as calm_device_id,
                    d.type as oem,
                    ft.subscription_id as free_trial_subscription_id,
                    ft.acnt_id as free_trial_acnt_id,
                    ft.purchased as free_trial_purchased_at,
                    p.subscription_id as paid_subscription_id,
                    p.acnt_id as paid_acnt_id,
                    p.purchased as paid_purchased_at,
                    p.total_proceeds as paid_total_proceeds,
                    p.product_proceeds_first_transaction as paid_product_proceeds_first_transaction,
                    p.product_price as paid_product_price,
                    p.refund_requested_at as paid_refund_requested_at,
                    row_number() over (partition by p.acnt_id order by af.install_time desc) as rn_paid_desc,
                    row_number() over (partition by ft.acnt_id order by af.install_time desc) as rn_free_trial_desc,
                    current_timestamp as table_created_at
             from get_appsflyer_installs af
             -- only include those who match from appsflyer devices to calm devices
             -- otherwise there would be no way to know if they converted
             join get_devices d
                  on d.device_id_to_match = af.device_id_to_match
             left join get_free_trial_devices ft
                  on ft.sub_linked_device_id = d.id
                  and ft.purchased >= af.install_time
             left join get_paid_devices p
                  on p.sub_linked_device_id = d.id
                  and p.purchased >= af.install_time
    )
    -- for each paid account, attribute only the most recent appsflyer install
    where (paid_acnt_id is null or rn_paid_desc = 1)
    -- for each free trial account, attribute only the most recent appsflyer install
    and (free_trial_acnt_id is null or rn_free_trial_desc = 1)
)
select *
from get_appsflyer_subscriptions;
commit;
