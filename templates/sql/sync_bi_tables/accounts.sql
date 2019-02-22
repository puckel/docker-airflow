DROP TABLE IF EXISTS {{ params.schema }}.accounts_stage;
CREATE TABLE {{ params.schema }}.accounts_stage
    distkey (acnt_id)
    interleaved sortkey (acnt_created_at,
                         has_user_id,
                         subscription_status,
                         session_activity_status,
                         subscription_paid_for_status)
AS
with devices_recent as (
    select acnt_id,
           device_id as recent_device_id,
           substring(locale, 1, 2) as recent_language,
    --store country code, which uses apples api, is way more accurate than cc, which use ip, for ios devices that have it
           cc as recent_cc,
           carrier as recent_carrier,
           platform as recent_device_platform,
           type as recent_device_type,
           os_version as recent_os_version,
           app_version as recent_app_version,
           last_ip as last_ip
    from {{ params.schema }}.device_acnts_stage
    where order_appeared_reversed = 1
),
devices_first as (
    select acnt_id,
           platform as first_seen_platform,
           first_seen as first_device_first_seen,
           device_id as first_seen_device_id
    from {{ params.schema }}.device_acnts_stage
    where order_appeared = 1
),
devices_cnt as (
    select acnt_id,
           count(*) as n_devices,
           case when count(distinct locale) = 0
                 then 1
                 else count(distinct locale)
           end as n_locales
    from {{ params.schema }}.device_acnts_stage
    group by acnt_id
),
sess_cnt as (
    select acnt_id,
            case when sum(n_sess) is null then 0
                else sum(n_sess) end as n_sess,
            sum( case when session_type = 'breathe' then n_sess
             else 0 end) as n_sess_breathe,
            sum( case when session_type = 'meditation' then n_sess
                 else 0 end) as n_sess_meditation,
            sum( case when session_type = 'masterclass' then n_sess
                 else 0 end) as n_sess_masterclass,
            sum( case when session_type = 'music' then n_sess
                 else 0 end) as n_sess_music,
            sum( case when session_type = 'sleep' then n_sess
                 else 0 end) as n_sess_sleep,
            sum(total_duration) as total_duration,
            sum( case when session_type = 'meditation' then total_duration
                 else 0 end) as tot_duration_meditation,
            sum( case when session_type = 'music' then total_duration
                 else 0 end) as tot_duration_music,
            sum( case when session_type = 'sleep' then total_duration
                 else 0 end) as tot_duration_sleep,
            sum( case when session_type = 'masterclass' then total_duration
                 else 0 end) as tot_duration_masterclass
    from (
        select acnt_id,
            session_type,
            count(logged_at) as n_sess,
            round(sum(duration) / 60, 1) as total_duration
        from {{ params.schema }}.sessions_all_stage
        group by 1, 2
    ) s
    group by s.acnt_id
),
sess_cnt_by_meditation_type as (
select acnt_id,
        case when sum(n_sess) is null then 0
            else sum(n_sess) end as n_sess,
        sum( case when meditation_type = 'timer' then n_sess
         else 0 end) as n_sess_timer,
        sum( case when meditation_type = 'freeform' then n_sess
             else 0 end) as n_sess_freeform,
        sum( case when meditation_type = 'sequential' then n_sess
             else 0 end) as n_sess_sequential,
        sum( case when meditation_type = 'daily_calm' then n_sess
             else 0 end) as n_sess_daily_calm,
        sum( case when meditation_type = 'series' then n_sess
             else 0 end) as n_sess_series
from (
    select acnt_id,
        meditation_type,
        count(logged_at) as n_sess
    from {{ params.schema }}.sessions_all_stage
    where session_type='meditation'
    group by 1, 2
) s
group by s.acnt_id
),
sess_day_part_cnt as (
    select acnt_id,
            sum( case when logged_part_of_day = 'morning' then n_sess
             else 0 end) as n_morning,
            sum( case when logged_part_of_day = 'afternoon' then n_sess
                 else 0 end) as n_afternoon,
            sum( case when logged_part_of_day = 'middle_night' then n_sess
                 else 0 end) as n_middle_night,
            sum( case when logged_part_of_day = 'evening' then n_sess
                 else 0 end) as n_evening
    from (
        select acnt_id,
            logged_part_of_day,
            count(logged_at) as n_sess
        from {{ params.schema }}.sessions_all_stage
        group by 1, 2
    ) s
    group by s.acnt_id
),
favorite_sleep_type as (
    select acnt_id, sleep_type as favorite_sleep_type
    from (
        select acnt_id,
                sleep_type,
                row_number() over (partition by acnt_id order by n_sess desc) as r_n
        from (
                select
                    acnt_id,
                    sleep_type,
                    count(*) as n_sess
                from {{ params.schema }}.sessions_all_stage
                where session_type = 'sleep'
                and progress > .95
                and acnt_id is not null
                and sleep_type is not null
                group by 1, 2
                having count(*) > 0
             )
        )
    where r_n = 1
),
favorite_music_type as (
    select acnt_id, music_type as favorite_music_type
    from (
        select acnt_id,
                music_type,
                row_number() over (partition by acnt_id order by n_sess desc) as r_n
        from (
                select
                    acnt_id,
                    music_type,
                    count(*) as n_sess
                from {{ params.schema }}.sessions_all_stage
                where session_type = 'music'
                and progress > .75
                and acnt_id is not null
                and music_type is not null
                group by 1, 2
                having count(*) > 0
             )
        )
    where r_n = 1
),
kid_sess as (
    select acnt_id, count(*) as n_kid_sessions
    from {{ params.schema }}.sessions_all_stage sess
    join (
        select distinct guide_variant_id
        from {{ params.schema }}.sessions_all_stage
        where lower(guide_title) like '%kid%'
        or lower(program_subtitle) like '%kid%'
        or lower(program_title) like '%kid%'
    ) kids
    on kids.guide_variant_id = sess.guide_variant_id
    group by 1
    having count(*) > 0
),
days_since_sess_type as (
    select s.acnt_id,
        min(days_since) as days_since_last_sess,
         sum( case when session_type = 'breathe_bubble' then days_since
             else null end) as days_since_breathe_bubble,
        sum( case when session_type = 'meditation' then days_since
             else null end) as days_since_meditation,
        sum( case when session_type = 'masterclass' then days_since
             else null end) as days_since_masterclass,
        sum( case when session_type = 'music' then days_since
             else null end) as days_since_music,
        sum( case when session_type = 'sleep' then days_since
             else null end) as days_since_sleep
    from (
        select acnt_id,
            session_type,
            min(days_since_sess) as days_since
        from {{ params.schema }}.sessions_all_stage
        group by 1, 2
    ) s
    group by s.acnt_id
),
days_since_meditation_type as (
    select s.acnt_id,
         sum( case when meditation_type = 'daily_calm' then days_since
             else null end) as days_since_daily_calm,
        sum( case when meditation_type = 'series' then days_since
             else null end) as days_since_series,
        sum( case when meditation_type = 'sequential' then days_since
             else null end) as days_since_sequential,
        sum( case when meditation_type = 'freeform' then days_since
             else null end) as days_since_freeform,
        sum( case when meditation_type = 'timer' then days_since
             else null end) as days_since_timer,
        sum( case when meditation_type = 'manual' then days_since
             else null end) as days_since_manual
    from (
        select acnt_id,
            meditation_type,
            min(days_since_sess) as days_since
        from {{ params.schema }}.sessions_all_stage
        where session_type='meditation'
        group by 1, 2
    ) s
    group by s.acnt_id
),
total_subs as (select acnt_id,
                        sum(n_transactions) as n_transactions,
                        round(sum(total_paid), 2) as total_paid
          from {{ params.schema }}.subscriptions_stage
          group by acnt_id
    )
select u.acnt_id,
    u.user_id,
    u.name,
    u.created_at as acnt_created_at,
    devices_first.first_device_first_seen as acnts_first_seen_device,
    u.updated_at as acnt_updated_at,
    datediff(days, u.created_at, getdate()) AS days_since_acnt_created,
    CASE
          WHEN u.user_id = 'NULL' or u.user_id is null THEN false
          WHEN u.user_id <> 'NULL' THEN true
          ELSE NULL
        END AS acnt_has_user_id,
    case when subs.plan is null and prev_subs.plan is null THEN 'never'
        when subs.plan is not null then 'subscribed'
        when subs.plan is null and prev_subs.plan is not null then 'churned'
        else 'error'
        END as subscription_status,
    case when subs.plan is null and prev_subs.plan is null THEN 'never'
        when subs.plan is null and prev_subs.plan is not null and prev_subs.plan_is_free = false THEN 'churned_from_paid_plan'
        when subs.plan is null and prev_subs.plan is not null and prev_subs.is_trial_period = true THEN 'churned_prev_subs_was_free_trial'
        when subs.plan is null and prev_subs.plan is not null and prev_subs.plan_is_free = true THEN 'churned_prev_subs_was_free_plan'
        when subs.plan is not null and subs.plan_is_free = false then 'subscribed_paid_plan'
        when subs.plan is not null and subs.is_trial_period = true then 'subscribed_free_trial'
        when subs.plan is not null and subs.plan_is_free = true then 'subscribed_on_free_plan'
        else 'error'
        END as subscription_paid_for_status,
    case when days_since_sess_type.days_since_last_sess > 30 then 'dormant'
        when days_since_sess_type.days_since_last_sess <= 30 then 'active'
        when days_since_sess_type.days_since_last_sess is null then 'never started'
        else 'error'
        END as session_activity_status,
    case when subs.plan is not null then true
        else false end as is_subscribed,

    case when prev_subs.plan is not null then true
        else false
        end as had_prev_subscription,

    case when u.facebook_user_id is not null then true
        else false
        end as has_fb_id,
    case when cu.college_id is not null then true
        else false
        end as in_calm_college,
    case when tm.team_id is not null then true
        else false
        end as on_a_team,
    case when  u.user_id is not null then true
        else false
        end as has_user_id,
    case when u.email_status = 1 then true
        when u.email_status = 0 then false
        end as has_email,
    case when fb.fb_gender = 'male' then 1
        when fb.fb_gender = 'female' then 0
        else u.probability_male end as probability_male,
    case when fb.fb_gender = 'male' then 'male'
        when fb.fb_gender = 'female' then 'female'
        when u.probability_male > .7 then 'male'
        when u.probability_male <= .3 and u.probability_male >= 0 then 'female'
        when u.probability_male < 0 then 'unknown_model_failed'
        when u.probability_male > .3 and u.probability_male < .7 then 'uncertain'
        else null
        end as gender,
    case when sess_cnt.n_sess_meditation > sess_cnt.n_sess_sleep
            and sess_cnt.n_sess_meditation > sess_cnt.n_sess_music
            and sess_cnt.n_sess_meditation > sess_cnt.n_sess_masterclass
         then 'meditation'
        when sess_cnt.n_sess_sleep > sess_cnt.n_sess_meditation
            and sess_cnt.n_sess_sleep > sess_cnt.n_sess_music
            and sess_cnt.n_sess_sleep > sess_cnt.n_sess_masterclass
         then 'sleep'
        when sess_cnt.n_sess_music > sess_cnt.n_sess_meditation
            and sess_cnt.n_sess_music > sess_cnt.n_sess_sleep
            and sess_cnt.n_sess_music > sess_cnt.n_sess_masterclass
         then 'music'
        when sess_cnt.n_sess_masterclass > sess_cnt.n_sess_meditation
            and sess_cnt.n_sess_masterclass > sess_cnt.n_sess_sleep
            and sess_cnt.n_sess_masterclass > sess_cnt.n_sess_music
         then 'masterclass'
        when sess_cnt.n_sess_breathe > sess_cnt.n_sess_meditation
            and sess_cnt.n_sess_breathe > sess_cnt.n_sess_sleep
            and sess_cnt.n_sess_breathe > sess_cnt.n_sess_music
            and sess_cnt.n_sess_breathe > sess_cnt.n_sess_masterclass
         then 'breathe'
        ELSE 'uncertain'
        END as favorite_session_type,
    case when sess_cnt_by_meditation_type.n_sess_daily_calm > sess_cnt_by_meditation_type.n_sess_timer
            and sess_cnt_by_meditation_type.n_sess_daily_calm > sess_cnt_by_meditation_type.n_sess_freeform
            and sess_cnt_by_meditation_type.n_sess_daily_calm > sess_cnt_by_meditation_type.n_sess_sequential
            and sess_cnt_by_meditation_type.n_sess_daily_calm > sess_cnt_by_meditation_type.n_sess_series
         then 'daily_calm'
        when sess_cnt_by_meditation_type.n_sess_timer > sess_cnt_by_meditation_type.n_sess_daily_calm
            and sess_cnt_by_meditation_type.n_sess_timer > sess_cnt_by_meditation_type.n_sess_freeform
            and sess_cnt_by_meditation_type.n_sess_timer > sess_cnt_by_meditation_type.n_sess_sequential
            and sess_cnt_by_meditation_type.n_sess_timer > sess_cnt_by_meditation_type.n_sess_series
         then 'timer'
        when sess_cnt_by_meditation_type.n_sess_freeform > sess_cnt_by_meditation_type.n_sess_timer
            and sess_cnt_by_meditation_type.n_sess_freeform > sess_cnt_by_meditation_type.n_sess_daily_calm
            and sess_cnt_by_meditation_type.n_sess_freeform > sess_cnt_by_meditation_type.n_sess_sequential
            and sess_cnt_by_meditation_type.n_sess_freeform > sess_cnt_by_meditation_type.n_sess_series
         then 'freeform'
        when sess_cnt_by_meditation_type.n_sess_sequential > sess_cnt_by_meditation_type.n_sess_timer
            and sess_cnt_by_meditation_type.n_sess_sequential > sess_cnt_by_meditation_type.n_sess_freeform
            and sess_cnt_by_meditation_type.n_sess_sequential > sess_cnt_by_meditation_type.n_sess_daily_calm
            and sess_cnt_by_meditation_type.n_sess_sequential > sess_cnt_by_meditation_type.n_sess_series
         then 'sequential'
         when sess_cnt_by_meditation_type.n_sess_series > sess_cnt_by_meditation_type.n_sess_timer
            and sess_cnt_by_meditation_type.n_sess_series > sess_cnt_by_meditation_type.n_sess_freeform
            and sess_cnt_by_meditation_type.n_sess_series > sess_cnt_by_meditation_type.n_sess_sequential
            and sess_cnt_by_meditation_type.n_sess_series > sess_cnt_by_meditation_type.n_sess_series
         then 'series'
        ELSE 'uncertain'
        END as favorite_meditation_type,
    case when favorite_sleep_type.favorite_sleep_type is not null then lower(favorite_sleep_type.favorite_sleep_type)
        else 'uncertain' end as favorite_sleep_type,
    case when favorite_music_type.favorite_music_type is not null then lower(favorite_music_type.favorite_music_type)
        else 'uncertain' end as favorite_music_type,
    case when sess_day_part_cnt.n_morning > sess_day_part_cnt.n_afternoon
            and sess_day_part_cnt.n_morning > sess_day_part_cnt.n_evening
            and sess_day_part_cnt.n_morning > sess_day_part_cnt.n_middle_night
         then 'morning'
        when sess_day_part_cnt.n_afternoon > sess_day_part_cnt.n_morning
            and sess_day_part_cnt.n_afternoon > sess_day_part_cnt.n_evening
            and sess_day_part_cnt.n_afternoon > sess_day_part_cnt.n_middle_night
         then 'afternoon'
        when sess_day_part_cnt.n_evening > sess_day_part_cnt.n_morning
            and sess_day_part_cnt.n_evening > sess_day_part_cnt.n_afternoon
            and sess_day_part_cnt.n_evening > sess_day_part_cnt.n_middle_night
         then 'evening'
        when sess_day_part_cnt.n_middle_night > sess_day_part_cnt.n_morning
            and sess_day_part_cnt.n_middle_night > sess_day_part_cnt.n_afternoon
            and sess_day_part_cnt.n_middle_night > sess_day_part_cnt.n_evening
         then 'middle_night'
        ELSE 'uncertain'
        END as favorite_part_of_day,
    case when kid_sess.n_kid_sessions > 2 then true
        else false end as likes_sessions_for_kids,
    case when kid_sess.n_kid_sessions is not null then n_kid_sessions
        else 0 end as n_kid_sessions,
    u.email,
    u.email_domain,
    fb.fb_email,
    fb.fb_first_name,
    fb.fb_last_name,
    fb.fb_birthdate,
    datediff(year, fb.fb_birthdate, getdate()) AS fb_age,
    case when ts.n_transactions is null then 0
        else ts.n_transactions
        end as n_transactions,
    ts.total_paid,
    coalesce(prev_subs.first_purchase, subs.first_purchase) as first_subs_purchased,
    subs.purchased as current_subs_purchased,
    subs.expires as current_subs_expires,
    subs.plan as current_plan,
    subs.plan_type as current_plan_type,
    subs.platform as current_purchased_platform,
    subs.plan_is_free as current_plan_is_free,
    subs.plan_is_free_for_user as current_plan_is_free_for_user,
    subs.is_trial_period as current_plan_is_trial_period,
    subs.is_in_billing_retry_period,
    subs.auto_renewing,
    subs.auto_renewal_canceled_at_stripe_only,
    subs.product_price as current_product_price,
    subs.campaign_name,
    prev_subs.purchased as prev_subs_purchased,
    prev_subs.expires as prev_subs_expires,
    prev_subs.plan as prev_plan,
    prev_subs.plan_type as prev_plan_type,
    prev_subs.platform as prev_purchased_platform,
    prev_subs.plan_is_free as prev_plan_was_free,
    prev_subs.plan_is_free_for_user as prev_plan_was_free_for_user,
    prev_subs.is_trial_period as prev_plan_is_trial_period,
    datediff(day, prev_subs.expires, getdate()) AS days_since_prev_subs_expired,
    case when subs.plan is null and prev_subs.plan is not null Then datediff(day, prev_subs.expires, getdate())
        else null end as days_since_churn,
    case when devices_cnt.n_devices is null then 0
        else devices_cnt.n_devices
        end as n_devices,
    devices_first.first_seen_device_id,
    devices_first.first_seen_platform,
    devices_recent.recent_device_id,
    devices_recent.recent_language,
    devices_recent.recent_cc,
    devices_recent.recent_carrier,
    devices_recent.recent_device_platform,
    devices_recent.recent_device_type,
    devices_recent.recent_os_version,
    devices_recent.recent_app_version,
    case when sess_cnt.n_sess is null then 0
        else sess_cnt.n_sess
        end as n_sess,
    case when sess_cnt.n_sess_meditation is null then 0
        else  sess_cnt.n_sess_meditation
        end  as n_sess_meditation,
    case when sess_cnt.n_sess_sleep is null then 0
        else  sess_cnt.n_sess_sleep
        end as n_sess_sleep,
    case when sess_cnt.n_sess_masterclass is null then 0
        else  sess_cnt.n_sess_masterclass
        end  as n_sess_masterclass,
    case when sess_cnt.n_sess_music is null then 0
        else  sess_cnt.n_sess_music
        end  as n_sess_music,
    case when sess_cnt.n_sess_breathe is null then 0
        else  sess_cnt.n_sess_breathe
        end  as n_sess_breathe,
    round(100 * (sess_cnt.n_sess_meditation / sess_cnt.n_sess::decimal), 1) as percent_sess_meditation,
    round(100 * (sess_cnt.n_sess_sleep / sess_cnt.n_sess::decimal), 1) as percent_sess_sleep,
    round(100 * (sess_cnt.n_sess_music / sess_cnt.n_sess::decimal), 1) as percent_sess_music,
    round(100 * (sess_cnt.n_sess_masterclass / sess_cnt.n_sess::decimal), 1) as percent_sess_masterclass,
    round(100 * (sess_cnt.n_sess_breathe / sess_cnt.n_sess::decimal), 1) as percent_sess_breath,
    sess_cnt_by_meditation_type.n_sess_daily_calm,
    sess_cnt_by_meditation_type.n_sess_timer,
    sess_cnt_by_meditation_type.n_sess_freeform,
    sess_cnt_by_meditation_type.n_sess_sequential,
    sess_cnt_by_meditation_type.n_sess_series,
    case when sess_cnt.total_duration is null then 0
        else sess_cnt.total_duration
        end as total_duration_minutes,
    case when sess_cnt.tot_duration_meditation is null then 0
        else sess_cnt.tot_duration_meditation
        end as tot_duration_meditation,
    case when sess_cnt.tot_duration_music is null then 0
        else sess_cnt.tot_duration_music
        end as tot_duration_music,
    case when sess_cnt.tot_duration_sleep is null then 0
        else sess_cnt.tot_duration_sleep
        end as tot_duration_sleep,
    case when sess_cnt.tot_duration_masterclass is null then 0
        else sess_cnt.tot_duration_masterclass
        end as tot_duration_masterclass,
    case when sess_day_part_cnt.n_morning is null then 0
        else  sess_day_part_cnt.n_morning
        end  as n_sess_morning,
    case when sess_day_part_cnt.n_afternoon is null then 0
        else  sess_day_part_cnt.n_afternoon
        end as n_sess_afternoon,
    case when sess_day_part_cnt.n_evening is null then 0
        else  sess_day_part_cnt.n_evening
        end  as n_sess_evening,
    case when sess_day_part_cnt.n_middle_night is null then 0
        else  sess_day_part_cnt.n_middle_night
        end  as n_sess_middle_night,
    days_since_sess_type.days_since_last_sess,
    days_since_sess_type.days_since_meditation,
    days_since_sess_type.days_since_sleep,
    days_since_sess_type.days_since_masterclass,
    days_since_sess_type.days_since_music,
    days_since_sess_type.days_since_breathe_bubble,
    days_since_meditation_type.days_since_daily_calm,
    days_since_meditation_type.days_since_series,
    days_since_meditation_type.days_since_sequential,
    days_since_meditation_type.days_since_freeform,
    days_since_meditation_type.days_since_timer,
    days_since_meditation_type.days_since_manual,
    devices_recent.last_ip,
    subs.details_id as current_subs_details_id,
    prev_subs.details_id as previous_subs_details_id,
    cu.college_id,
    cu.college_created_at,
    tm.team_id,
    tm.team_member_created_at,
    t.team_name,
    t.team_created_at,
    current_timestamp as table_created_at
from (
        (select
            id as acnt_id,
            id as user_id,
            null as device_id,
            email,
            split_part(split_part(email, '@', 2), '.', 1) as email_domain,
            name,
            created_at,
            updated_at,
            facebook_user_id,
            email_status,
            probability_male
        from appdb.users
        )

        UNION ALL

        (select
            device_id as acnt_id,
            null as user_id,
            device_id,
            null as email,
            null as email_domain,
            null as name,
            first_seen as created_at,
            null as updated_at,
            null as facebook_user_id,
            0 as email_status,
            .5 as probability_male
        from {{ params.schema }}.device_acnts_stage
        where user_id is null
        )
    ) u
left join (select
        id,
        first_name as fb_first_name,
        last_name as fb_last_name,
        email as fb_email,
        gender as fb_gender,
        birthdate as fb_birthdate,
        created_at as fb_created_at
        from appdb.facebook_users
        ) fb
on fb.id = u.facebook_user_id
left join (select
                user_id,
                college_id,
                created_at as college_created_at
         from appdb.college_users
         ) cu
on cu.user_id = u.user_id
left join (select
                team_id,
                user_id,
                created_at as team_member_created_at
         from appdb.team_members
         ) tm
on tm.user_id = u.user_id
left join (select
                id,
                name as team_name,
                created_at as team_created_at
            from appdb.teams
            ) t
on t.id = tm.team_id
left join devices_recent
on devices_recent.acnt_id = u.acnt_id
left join devices_first
on devices_first.acnt_id = u.acnt_id
left join devices_cnt
on devices_cnt.acnt_id = u.acnt_id
left join sess_cnt
on sess_cnt.acnt_id = u.acnt_id
left join sess_cnt_by_meditation_type
on sess_cnt_by_meditation_type.acnt_id = u.acnt_id
left join days_since_sess_type
on days_since_sess_type.acnt_id = u.acnt_id
left join days_since_meditation_type
on days_since_meditation_type.acnt_id = u.acnt_id
left join (
    select *
    from (select *,
                row_number() over (partition BY acnt_id
                               ORDER BY expires desc) row_num
          from {{ params.schema }}.subscriptions_stage
           where expires > getdate()
          ) current_subs
    where current_subs.row_num = 1
    ) subs
on subs.acnt_id = u.acnt_id
left join (
    select *
    from (select *,
                row_number() over (partition BY acnt_id
                               ORDER BY expires desc) row_num
          from {{ params.schema }}.subscriptions_stage
           where expires < getdate()
          ) current_subs
    where current_subs.row_num = 1
    ) prev_subs
on prev_subs.acnt_id = u.acnt_id
left join total_subs ts
on ts.acnt_id = u.acnt_id
left join sess_day_part_cnt
on sess_day_part_cnt.acnt_id = u.acnt_id
left join favorite_sleep_type
on favorite_sleep_type.acnt_id = u.acnt_id
left join favorite_music_type
on favorite_music_type.acnt_id = u.acnt_id
left join kid_sess
on kid_sess.acnt_id = u.acnt_id
;
