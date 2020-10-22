drop table if exists ab_platform.experiment_populations_activities_on_app_open_temp cascade;

create table ab_platform.experiment_populations_activities_on_app_open_temp
  distkey (entity_id)
  sortkey (entity_id) as (
  select
    'e93d8cb8-dc32-4e44-8629-a2c00bdd8822'                                       as experiment_id,
    eventvalue as variant,
    entityid                                                                     as entity_id,
    'Parents'                                                                    as entity_type,
    min(createdat)                                                               as entered_at
  from logs.product_event_no_pii
  where
    eventname = 'ios.experiment.iOS_parentActivitiesOnAppOpen'
    and createdat >= '2020-09-28'
    and createdat <= getdate()
    and eventvalue != 'off'
  group by
    entity_id,
    variant,
    experiment_id,
    entity_type
);

begin;
grant all on ab_platform.experiment_populations_activities_on_app_open_temp to group team;
grant all on ab_platform.experiment_populations_activities_on_app_open_temp to astronomer;
drop table if exists ab_platform.experiment_populations_activities_on_app_open cascade;
alter table ab_platform.experiment_populations_activities_on_app_open_temp
  rename to experiment_populations_activities_on_app_open;
commit;
