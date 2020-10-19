drop table if exists ab_platform.experiment_populations_parents_who_havent_visited_beyond_tab_temp cascade;

create table ab_platform.experiment_populations_parents_who_havent_visited_beyond_tab_temp
  distkey (entity_id)
  sortkey (entity_id) as (
  select
    'd7439d5c-4f04-42ed-a20e-ec7fe3834b93'                                       as experiment_id,
    eventvalue as variant,
    entityid                                                                     as entity_id,
    'Parents'                                                                    as entity_type,
    min(createdat)                                                               as entered_at
  from logs.product_event_no_pii
  where
    eventname = 'ios.parent.freemium.beyond.nux.exposure'
    and createdat >= '2020-09-18'
    and createdat <= getdate()
    and eventvalue != 'off'
    and eventvalue != ''
  group by
    entity_id,
    variant,
    experiment_id,
    entity_type
);

begin;
grant all on ab_platform.experiment_populations_parents_who_havent_visited_beyond_tab_temp to group team;
grant all on ab_platform.experiment_populations_parents_who_havent_visited_beyond_tab_temp to astronomer;
drop table if exists ab_platform.experiment_populations_parents_who_havent_visited_beyond_tab cascade;
alter table ab_platform.experiment_populations_parents_who_havent_visited_beyond_tab_temp
  rename to experiment_populations_parents_who_havent_visited_beyond_tab;
commit;
