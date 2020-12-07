drop table if exists ab_platform.experiment_populations_ios_parent_nav_simplification_temp cascade;

create table ab_platform.experiment_populations_ios_parent_nav_simplification_temp
  distkey (entity_id)
  sortkey (entity_id) as (
  select
    '1c510a58-6e38-4f30-9181-f5be15cb4208'                                       as experiment_id,
    eventvalue as variant,
    entityid                                                                     as entity_id,
    'Parents'                                                                    as entity_type,
    min(createdat)                                                               as entered_at
  from logs.product_event_no_pii
  where
    eventname = 'ios.experiment.ios_parentNavSimplificationV01'
    and createdat >= '2020-12-07'
    and createdat <= getdate()
    and eventvalue != 'off'
  group by
    entity_id,
    variant,
    experiment_id,
    entity_type
);

begin;
grant all on ab_platform.experiment_populations_ios_parent_nav_simplification_temp to group team;
grant all on ab_platform.experiment_populations_ios_parent_nav_simplification_temp to astronomer;
drop table if exists ab_platform.experiment_populations_ios_parent_nav_simplification cascade;
alter table ab_platform.experiment_populations_ios_parent_nav_simplification_temp
  rename to experiment_populations_ios_parent_nav_simplification;
commit;
