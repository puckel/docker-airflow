drop table if exists ab_platform.experiment_populations_android_freemium_v2_temp cascade;

create table ab_platform.experiment_populations_android_freemium_v2_temp
  distkey (entity_id)
  sortkey (entity_id) as (
  select
    '23f89995-a6e5-4640-8652-f5dfb2559a9e'                                       as experiment_id,
    eventvalue as variant,
    entityid                                                                     as entity_id,
    'Parents'                                                                    as entity_type,
    min(createdat)                                                               as entered_at
  from logs.product_event_no_pii
  where
    eventname = 'android.parent.freemium.beyondTab.exposure '
    and createdat >= '2020-10-26'
    and createdat <= getdate()
    and eventvalue IN ('test', 'control')
  group by
    entity_id,
    variant,
    experiment_id,
    entity_type
);

begin;
grant all on ab_platform.experiment_populations_android_freemium_v2_temp to group team;
grant all on ab_platform.experiment_populations_android_freemium_v2_temp to astronomer;
drop table if exists ab_platform.experiment_populations_android_freemium_v2 cascade;
alter table ab_platform.experiment_populations_android_freemium_v2_temp
  rename to experiment_populations_android_freemium_v2;
commit;
