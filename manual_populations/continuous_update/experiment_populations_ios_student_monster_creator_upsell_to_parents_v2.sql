/* Intermediate table that shows all students and their variant */
drop table if exists ab_platform.experiment_populations_ios_student_monster_creator_upsell_student_population_temp cascade;

create table ab_platform.experiment_populations_ios_student_monster_creator_upsell_student_population_temp
  distkey (pstudentid) as (
  select
    events.entityid                   as pstudentid,
    events.eventvalue                 as variant,
    min(createdat)                    as entered_at,
    date_trunc('day', min(createdat)) as entered_at_day
  from logs.product_event_no_pii events
  where
    eventname = 'ios.experiment.iOS_studentMonsterCreatorUpsell'
    and createdat >= '2020-09-29'
    and events.eventvalue != 'off'
  group by 1, 2
);

begin;
grant all on ab_platform.experiment_populations_ios_student_monster_creator_upsell_student_population_temp to group team;
grant all on ab_platform.experiment_populations_ios_student_monster_creator_upsell_student_population_temp to astronomer;
drop table if exists ab_platform.experiment_populations_ios_student_monster_creator_upsell_student_population cascade;
alter table ab_platform.experiment_populations_ios_student_monster_creator_upsell_student_population_temp
  rename to experiment_populations_ios_student_monster_creator_upsell_student_population;
commit;

/*
Final population table that selects all parents who are only connected
to one kid, then joins only kids in the experiment to get the parent
variant.
*/
drop table if exists ab_platform.experiment_populations_ios_student_monster_creator_upsell_to_parents_v2_temp cascade;

create table ab_platform.experiment_populations_ios_student_monster_creator_upsell_to_parents_v2_temp
  distkey (entity_id)
  sortkey (entity_id) as (

  with parent_one_child as (
      select
        graph.parentid,
        count(distinct pstudentid) as students_connected
      from cache.graph
      group by 1
      having students_connected = 1
  )

  select
    '06995fe0-8868-4ee5-a3f4-66f6beb466fc' as experiment_id,
    pop.variant                            as variant,
    pone.parentid                          as entity_id,
    'Parents'                              as entity_type,
    min(pop.entered_at)                    as entered_at
  from parent_one_child pone
    join cache.graph on graph.parentid = pone.parentid
    join ab_platform.experiment_populations_ios_student_monster_creator_upsell_student_population pop
      on pop.pstudentid = graph.pstudentid
  group by 1, 2, 3, 4
);

begin;
grant all on ab_platform.experiment_populations_ios_student_monster_creator_upsell_to_parents_v2_temp to group team;
grant all on ab_platform.experiment_populations_ios_student_monster_creator_upsell_to_parents_v2_temp to astronomer;
drop table if exists ab_platform.experiment_populations_ios_student_monster_creator_upsell_to_parents_v2 cascade;
alter table ab_platform.experiment_populations_ios_student_monster_creator_upsell_to_parents_v2_temp
  rename to experiment_populations_ios_student_monster_creator_upsell_to_parents_v2;
commit;
