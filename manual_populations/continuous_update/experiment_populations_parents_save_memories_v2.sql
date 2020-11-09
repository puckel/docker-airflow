drop table if exists ab_platform.experiment_populations_parents_save_memories_v2_temp;
create table ab_platform.experiment_populations_parents_save_memories_v2_temp
  distkey(entity_id)
  sortkey(entity_id)
  as (
    SELECT '01858a04-5ffa-4a43-bbb5-1cf832867b7e' AS experiment_id
         , eventvalue                             as variant
         , entityid                               AS entity_id
         , 'Parents'                              AS entity_type
         , min(createdat)                         AS entered_at
    from logs.product_event_no_pii
    where eventname = 'ios.experiment.iOS_parentYearbookStoryFeed'
      and eventvalue in ('testV2', 'controlV2')
      and createdat >= '2020-11-13'
    GROUP BY 1, 2, 3, 4
);
begin;
grant all on ab_platform.experiment_populations_parents_save_memories_v2_temp to group team;
grant all on ab_platform.experiment_populations_parents_save_memories_v2_temp to astronomer;
drop table if exists ab_platform.experiment_populations_parents_save_memories_v2 cascade;
alter table ab_platform.experiment_populations_parents_save_memories_v2_temp rename to experiment_populations_parents_save_memories_v2;
commit;
