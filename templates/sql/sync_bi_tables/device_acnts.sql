/*
Create a devices table that contains acnt_id, user_id of the device if it exits
  -table also at a higher level so, even if sources of data change/added/removed,
    we can have stable estimates of
      -language,
      -cc (country code)
      -etc
  -device type first seen date
    -'2015-04-27' seems to be earliest date in our backend
        -but phones can, of course, be released earlier than that

-v2
  1) remove www devices if not in sessions
  2) integrage mParticle data
    -better information on device type; location (eg city, etc)

  3) will have extra information about devices e.g.,
    -age of device
    -device type first seen

    -cost of device (probably need outside sources)
*/

DROP TABLE IF EXISTS {{ params.schema }}.device_acnts_stage;
CREATE TABLE {{ params.schema }}.device_acnts_stage
distkey (acnt_id)
interleaved sortkey (order_appeared,
        order_appeared_reversed,
        first_seen,
        last_seen)
AS
with devs as (--additional info about a persons device
    SELECT coalesce(du.user_id, d.device_id) as acnt_id,
            du.user_id,
            d.device_id,
            d.platform,
            du.first_seen as first_seen_w_user_id,
            du.last_seen as last_seen_w_user_id,
            d.first_seen,
            d.last_seen,
            --store country code is best estimate
            coalesce(case when d.first_store_country_code = 'legacy'
              then null else d.first_store_country_code end,
              d.cc) as cc_estimated,
            case when d.first_store_country_code = 'legacy' then false
                when d.first_store_country_code is null then false
                when d.first_store_country_code is not null then true
                end as store_country_is_known,
            d.first_store_country_code as store_country_code, --relatively stable knowledge about persons country code
            d.cc as recent_cc, --based on most recent IP; changes!
            split_part(d.locale, '_', 2) as language_setting,
            d.accept_language,
            d.locale,
            d.carrier,
            d.connection_type,
            d.type,
            d.os_version,
            d.app_version,
            d.last_ip,
            d.name as personal_name,
            timezone
    FROM (select id as device_id,
            *
            from appdb.devices
            ) d
    FULL OUTER JOIN (select *
            from appdb.device_users
            ) du
    on du.device_id = d.id
),
devs_order as (
   select *,
        language_setting as language_estimated,
        row_number() over (partition by acnt_id order by last_seen desc) order_appeared_reversed,
        row_number() over (partition by acnt_id order by first_seen asc) order_appeared
    from devs
),
type_first_seen as (--for non-web devices;
    --get when device was first seen but only use days w/ 100 new devices
    --there are bugs in the DB that give a few devices wrong first_seen (earlier than possible)
    --www doesn't have useful device data; need to integrate from mparticle
    select type,
          min(first_seen) as device_type_first_seen
    from (
        select type,
              min(first_seen) as first_seen
        from appdb.devices
        where platform <> 'www'
        group by 1
        having count(*) > 100
    )
    group by 1
)
SELECT acnt_id,
        device_id,
        user_id,
        platform,
        order_appeared,
        order_appeared_reversed,
        first_seen_w_user_id,
        last_seen_w_user_id,
        first_seen,
        case when valid_tzs.name is null then null
            else CONVERT_TIMEZONE ( 'utc', valid_tzs.name, first_seen)  end as first_seen_local,
        last_seen,
        case when valid_tzs.name is null then null
            else CONVERT_TIMEZONE ( 'utc', valid_tzs.name, last_seen)  end as last_seen_local,
        lower(cc_estimated) as cc,
        store_country_is_known,
        language_estimated as languages,
        carrier,
        connection_type,
        d.type,
        t.device_type_first_seen,
        os_version,
        app_version,
        last_ip,
        locale,
        personal_name,
        store_country_code,
        recent_cc,
        languages as languages_devices_table,
        language_setting,
        accept_language,
        current_timestamp as table_created_at,
        --if timezone in devices is not RS compatible will be null
        valid_tzs.name as timezone,
        timezone as original_timezone_including_invalids
from devs_order d
left join type_first_seen t
on t.type = d.type
left join helper.valid_timezone_info valid_tzs
on valid_tzs.name = d.timezone
;
