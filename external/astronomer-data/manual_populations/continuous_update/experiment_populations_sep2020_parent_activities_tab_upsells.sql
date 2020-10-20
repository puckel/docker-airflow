drop table if exists ab_platform.experiment_populations_sep2020_parent_activities_tab_upsells_temp cascade;

create table ab_platform.experiment_populations_sep2020_parent_activities_tab_upsells_temp
  distkey (entity_id)
  sortkey (entity_id) as (
  select
    '672de3f3-12d9-4bf1-af08-e091c3ca4bd3'                                       as experiment_id,
    eventvalue as variant, 
    entityid                                                                     as entity_id,
    'Parents'                                                                    as entity_type,
    min(createdat)                                                               as entered_at
  from logs.product_event_no_pii
  where
    eventname = 'ios.experiment.iOS_parentActivitiesTabUpsells'
    and createdat >= '2020-09-30'
    and createdat <= getdate()
  group by
    entity_id,
    variant,
    experiment_id,
    entity_type
);

begin;
grant all on ab_platform.experiment_populations_sep2020_parent_activities_tab_upsells_temp to group team;
grant all on ab_platform.experiment_populations_sep2020_parent_activities_tab_upsells_temp to astronomer;
drop table if exists ab_platform.experiment_populations_sep2020_parent_activities_tab_upsells cascade;
alter table ab_platform.experiment_populations_sep2020_parent_activities_tab_upsells_temp
  rename to experiment_populations_sep2020_parent_activities_tab_upsells;
commit;
