-- combines facebook api aggregate data for a given ad and date
-- with appsflyer+calm conversion aggregate data for a given facebook ad and date
-- so that each row represents aggregate conversion metrics for each facebook ad active on each date
begin;
drop table if exists {{ params.schema }}.facebook_conversions_summary_stage;
create table {{ params.schema }}.facebook_conversions_summary_stage
distkey (date_start_pt)
interleaved sortkey (date_start_pt,
                     platform,
                     campaign_name,
                     adset_name,
                     ad_name)
as
with get_facebook as (
    select ai.account_id,
           ai.ad_id,
           ai.ad_name,
           ai.adset_id,
           ai.adset_name,
           ai.campaign_id,
           ai.campaign_name,
           ai.clicks,
           ai.date_start as date_start_pt,
           ai.date_stop as date_stop_pt,
           ai.frequency,
           ai.impressions,
           ai.spend,
           ai.reach,
           ai.inline_link_clicks,
           ai.relevance_score__status,
           ai.inline_post_engagement,
           ai.relevance_score__score,
           ai.unique_clicks,
           ai.canvas_avg_view_percent,
           ai.canvas_avg_view_time,
           coalesce(aia.value, 0) as n_installs_fb,
           coalesce(aiua.value, 0) as n_unique_installs_fb,
           -- infer platform from campaign name while we are waiting for stitch platform data to be ingested (fb.ads_insights_platform_and_device)
           case when campaign_name ilike '%\\_ios%' then 'ios'
                when campaign_name ilike '%\\_andr%' then 'android'
                else 'unknown'
           end as platform
    from fb.ads_insights ai
    left join fb.ads_insights__actions aia
        on aia._sdc_source_key_ad_id = ai.ad_id
        and aia._sdc_source_key_date_start = ai.date_start
        and aia.action_type = 'mobile_app_install'
    left join fb.ads_insights__unique_actions aiua
        on aiua._sdc_source_key_ad_id = ai.ad_id
        and aiua._sdc_source_key_date_start = ai.date_start
        and aiua.action_type = 'mobile_app_install'
),
get_appsflyer_conversions as (
    select *
    from {{ params.schema }}.appsflyer_conversions_stage
    where media_source = 'Facebook Ads'
),
get_free_trial_sess_counts_first_2d as (
    -- use sessions first 2d free trial as early indicator of whether free trial will convert in 8d from attributed touch time
    select af.free_trial_acnt_id,
           -- from d1 to d2 (48 hours), num distinct days completed sessions (0, 1, or 2)
           count(distinct extract('day' from (s.logged_at - af.free_trial_purchased_at))) as days_completed_sess_first_2d_free_trial
    from get_appsflyer_conversions af
    left join {{ params.schema }}.sessions_all_stage s
        on s.acnt_id = af.free_trial_acnt_id
        and s.logged_at between af.free_trial_purchased_at and af.free_trial_purchased_at + interval '2 days' -- activity in first 2 days of free trial only
        and s.logged_at <= af.attributed_touch_time + interval '6 days' -- only look at 2 days starting up to 6 days from install since we're predicting payments at 8d
        and s.completed = true -- only completed sessions
    where af.free_trial_acnt_id is not null
    and af.free_trial_purchased_at <= (select min(table_created_at) from {{ params.schema }}.device_acnts_stage) - interval '2 days' -- must have had 2+ days since free trial start
    group by 1
),
get_appsflyer_conversions_info as (
    select *,
           (attributed_touch_time at time zone 'utc' at time zone 'America/Los_Angeles') as attributed_touch_time_pt,
           attributed_touch_time_pt::date as attributed_touch_date_pt,
           (select min(table_created_at at time zone 'utc' at time zone 'America/Los_Angeles') from {{ params.schema }}.device_acnts_stage) as last_observed_pt,
           extract('day' from (last_observed_pt - attributed_touch_time_pt)) as days_observed,
           -- combine ads with and without .0 at end, can remove once https://github.com/calm/data-issues/issues/178 is fixed
           case when ad_id like '%.0'
                then replace(ad_id, '.0', '')
                else ad_id
           end as ad_id_fixed,
           extract('day' from (free_trial_purchased_at - attributed_touch_time)) as days_to_free_trial,
           extract('day' from (paid_purchased_at - attributed_touch_time)) as days_to_payment,
           extract('day' from (paid_refund_requested_at - attributed_touch_time)) as days_to_refund,
           -- 0s and 1s for whether they did free trial by a certain time cutoff
           case when days_observed >= 1
                then case when days_to_free_trial < 1
                          then 1
                          else 0
                     end
           end as is_free_trial_1d,
           case when days_observed >= 2
                then case when days_to_free_trial < 2
                          then 1
                          else 0
                     end
           end as is_free_trial_2d,
           case when days_observed >= 3
                then case when days_to_free_trial < 3
                          then 1
                          else 0
                     end
           end as is_free_trial_3d,
           case when days_observed >= 8
                then case when days_to_free_trial < 8
                          then 1
                          else 0
                     end
           end as is_free_trial_8d,
           case when days_to_free_trial is not null
                then 1
                else 0
           end is_free_trial_ever,
           -- 0s and 1s for whether they paid and weren't refunded by a certain time cutoff
          case when days_observed >= 8
               then case when days_to_payment < 8
                         and (paid_refund_requested_at is null
                              or days_to_refund >= 8)
                         then 1
                         else 0
                    end
          end as is_paid_8d,
          case when days_observed >= 10
               then case when days_to_payment < 10
                         and (paid_refund_requested_at is null
                              or days_to_refund >= 10)
                         then 1
                         else 0
                    end
          end as is_paid_10d,
          case when days_observed >= 14
               then case when days_to_payment < 14
                         and (paid_refund_requested_at is null
                              or days_to_refund >= 14)
                         then 1
                         else 0
                    end
          end as is_paid_14d,
          case when days_to_payment is not null
               and days_to_refund is null
               then 1
               else 0
          end as is_paid_ever,
           -- 0s and 1s for engagement level in first 2d
           case when sc.free_trial_acnt_id is not null
                then 1
                else 0
           end as is_measured_sess_first_2d_free_trial,
           case when sc.free_trial_acnt_id is not null
                then case when sc.days_completed_sess_first_2d_free_trial = 0
                          then 1
                          else 0
                     end
           end as is_0d_sess_first_2d_free_trial,
           case when sc.free_trial_acnt_id is not null
                then case when sc.days_completed_sess_first_2d_free_trial = 1
                          then 1
                          else 0
                     end
           end as is_1d_sess_first_2d_free_trial,
           case when sc.free_trial_acnt_id is not null
                then case when sc.days_completed_sess_first_2d_free_trial = 2
                          then 1
                          else 0
                     end
           end as is_2d_sess_first_2d_free_trial,
           -- 0s and 1s for payment by engagement level in first 2d
           case when days_observed >= 8
                and is_0d_sess_first_2d_free_trial = 1
                then is_paid_8d
           end as is_0d_sess_first_2d_free_trial_paid_8d,
           case when days_observed >= 8
                and is_1d_sess_first_2d_free_trial = 1
                then is_paid_8d
           end as is_1d_sess_first_2d_free_trial_paid_8d,
           case when days_observed >= 8
                and is_2d_sess_first_2d_free_trial = 1
                then is_paid_8d
           end as is_2d_sess_first_2d_free_trial_paid_8d,
          -- proceeds first transaction by different cutoffs
          case when days_observed >= 8
               then case when is_paid_8d = 1
                         then paid_product_proceeds_first_transaction
                         else 0
                    end
          end as paid_product_proceeds_first_transaction_8d,
          case when days_observed >= 10
               then case when is_paid_10d = 1
                         then paid_product_proceeds_first_transaction
                         else 0
                    end
          end as paid_product_proceeds_first_transaction_10d,
          case when days_observed >= 14
               then case when is_paid_14d = 1
                         then paid_product_proceeds_first_transaction
                         else 0
                    end
          end as paid_product_proceeds_first_transaction_14d,
          case when is_paid_ever
               then paid_product_proceeds_first_transaction
               else 0
          end as paid_product_proceeds_first_transaction_ever,
          -- 0s and 1s for whether they were free trial to paid and weren't refunded by a certain time cutoff
          case when free_trial_purchased_at <= paid_purchased_at
               then 1
               else 0
          end as is_free_trial_to_paid_ever,
          case when days_observed >= 8
               then case when is_paid_8d = 1
                         and is_free_trial_to_paid_ever = 1
                         then 1
                         else 0
                    end
          end as is_free_trial_to_paid_8d,
          case when days_observed >= 10
               then case when is_paid_10d = 1
                         and is_free_trial_to_paid_ever = 1
                         then 1
                         else 0
                    end
          end as is_free_trial_to_paid_10d,
          case when days_observed >= 14
               then case when is_paid_14d = 1
                         and is_free_trial_to_paid_ever = 1
                         then 1
                         else 0
                    end
          end as is_free_trial_to_paid_14d
    from get_appsflyer_conversions af
    left join get_free_trial_sess_counts_first_2d sc
        on sc.free_trial_acnt_id = af.free_trial_acnt_id
),
get_facebook_appsflyer_conversions_by_date as (
    select attributed_touch_date_pt,
           ad_id_fixed,
           min(days_observed) as days_observed,
           count(*) as n_installs_appsflyer_calm,
           sum(is_free_trial_1d) as n_free_trials_1d,
           sum(is_free_trial_2d) as n_free_trials_2d,
           sum(is_free_trial_3d) as n_free_trials_3d,
           sum(is_free_trial_8d) as n_free_trials_8d,
           sum(is_free_trial_ever) as n_free_trials_ever,
           sum(is_paid_8d) as n_paid_8d,
           sum(is_paid_10d) as n_paid_10d,
           sum(is_paid_14d) as n_paid_14d,
           sum(is_paid_ever) as n_paid_ever,
           sum(is_free_trial_to_paid_8d) as n_free_trials_paid_8d,
           sum(is_free_trial_to_paid_10d) as n_free_trials_paid_10d,
           sum(is_free_trial_to_paid_14d) as n_free_trials_paid_14d,
           sum(is_free_trial_to_paid_ever) as n_free_trials_paid_ever,
           avg(paid_product_price::float) as avg_product_price,
           sum(paid_product_proceeds_first_transaction_8d) as total_proceeds_first_transaction_8d,
           sum(paid_product_proceeds_first_transaction_10d) as total_proceeds_first_transaction_10d,
           sum(paid_product_proceeds_first_transaction_14d) as total_proceeds_first_transaction_14d,
           sum(paid_product_proceeds_first_transaction_ever) as total_proceeds_first_transaction_ever,
           sum(is_measured_sess_first_2d_free_trial) as n_measured_sess_first_2d_free_trial,
           sum(is_0d_sess_first_2d_free_trial) as n_0d_sess_first_2d_free_trial,
           sum(is_1d_sess_first_2d_free_trial) as n_1d_sess_first_2d_free_trial,
           sum(is_2d_sess_first_2d_free_trial) as n_2d_sess_first_2d_free_trial,
           sum(is_0d_sess_first_2d_free_trial_paid_8d) as n_0d_sess_first_2d_free_trial_paid_8d,
           sum(is_1d_sess_first_2d_free_trial_paid_8d) as n_1d_sess_first_2d_free_trial_paid_8d,
           sum(is_2d_sess_first_2d_free_trial_paid_8d) as n_2d_sess_first_2d_free_trial_paid_8d
    from get_appsflyer_conversions_info
    group by 1,2
),
get_conversion_counts as (
    select f.*,
           c.days_observed,
           -- coalesce to 0 only for unbounded metrics, for which we know null actually does mean 0 (rather than unknown)
           coalesce(c.n_installs_appsflyer_calm, 0) as n_installs_appsflyer_calm,
           c.n_free_trials_1d,
           c.n_free_trials_2d,
           c.n_free_trials_3d,
           c.n_free_trials_8d,
           coalesce(c.n_free_trials_ever, 0) as n_free_trials_ever,
           c.n_paid_8d,
           c.n_paid_10d,
           c.n_paid_14d,
           coalesce(c.n_paid_ever, 0) as n_paid_ever,
           c.n_free_trials_paid_8d,
           c.n_free_trials_paid_10d,
           c.n_free_trials_paid_14d,
           coalesce(c.n_free_trials_paid_ever, 0) as n_free_trials_paid_ever,
           c.avg_product_price,
           c.total_proceeds_first_transaction_8d,
           c.total_proceeds_first_transaction_10d,
           c.total_proceeds_first_transaction_14d,
           coalesce(c.total_proceeds_first_transaction_ever, 0) as total_proceeds_first_transaction_ever,
           c.n_measured_sess_first_2d_free_trial,
           c.n_0d_sess_first_2d_free_trial,
           c.n_1d_sess_first_2d_free_trial,
           c.n_2d_sess_first_2d_free_trial,
           c.n_0d_sess_first_2d_free_trial_paid_8d,
           c.n_1d_sess_first_2d_free_trial_paid_8d,
           c.n_2d_sess_first_2d_free_trial_paid_8d,

           -- hardcoded prediction coefficients from free trials purchased between '2018-10-01' and '2019-01-17' (source: https://modeanalytics.com/calm/reports/9d0c7f983c8f/notebook)
           case f.platform
                when 'android'
                then .40
                when 'ios'
                then .38
           end::float as platform_historical_pct_paid_8d,

           case f.platform
                when 'android'
                then .28
                when 'ios'
                then .29
           end::float as platform_0d_sess_first_2d_free_trial_historical_pct_paid_8d,

           case f.platform
                when 'android'
                then .39
                when 'ios'
                then .38
           end::float as platform_1d_sess_first_2d_free_trial_historical_pct_paid_8d,

           case f.platform
                when 'android'
                then .59
                when 'ios'
                then .55
           end::float as platform_2d_sess_first_2d_free_trial_historical_pct_paid_8d,

           c.n_free_trials_2d * platform_historical_pct_paid_8d as predict_n_free_trials_2d_will_pay_8d_from_platform_historical,

           c.n_0d_sess_first_2d_free_trial * platform_0d_sess_first_2d_free_trial_historical_pct_paid_8d +
              c.n_1d_sess_first_2d_free_trial * platform_1d_sess_first_2d_free_trial_historical_pct_paid_8d +
              c.n_2d_sess_first_2d_free_trial * platform_2d_sess_first_2d_free_trial_historical_pct_paid_8d
              as predict_n_free_trials_2d_will_pay_8d_from_platform_sess_historical,

           current_timestamp as table_created_at
    from get_facebook f
    left join get_facebook_appsflyer_conversions_by_date c
        on c.ad_id_fixed = f.ad_id
        and c.attributed_touch_date_pt between f.date_start_pt and f.date_stop_pt
)
select *
from get_conversion_counts;
commit;
