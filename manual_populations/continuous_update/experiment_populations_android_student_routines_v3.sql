/* Intermediate table that shows all students and their variant */
drop table if exists ab_platform.experiment_populations_android_student_routines_student_population_temp cascade;

create table ab_platform.experiment_populations_android_student_routines_student_population_temp
  distkey (pstudentid) as (
  select
    events.entityid                   as pstudentid,
    events.eventvalue                 as variant,
    min(createdat)                    as entered_at,
    date_trunc('day', min(createdat)) as entered_at_day
  from logs.product_event_no_pii events
  where
    eventname = 'android.student.home.routine.exposure'
    and createdat >= '2020-10-27'
    and createdat <= getdate()
    and events.eventvalue IN ('test', 'control')
  group by 1, 2
);

begin;
grant all on ab_platform.experiment_populations_android_student_routines_student_population_temp to group team;
grant all on ab_platform.experiment_populations_android_student_routines_student_population_temp to astronomer;
drop table if exists ab_platform.experiment_populations_android_student_routines_student_population cascade;
alter table ab_platform.experiment_populations_android_student_routines_student_population_temp
  rename to experiment_populations_android_student_routines_student_population;

/*
Final population table that selects all parents who are only connected
to one kid, then joins only kids in the experiment to get the parent
variant.
*/
drop table if exists ab_platform.experiment_populations_android_student_routines_to_parents_v3_temp cascade;

create table ab_platform.experiment_populations_android_student_routines_to_parents_v3_temp
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
    'cd02e0b2-205e-4782-ae02-0f308fa786f4' as experiment_id,
    pop.variant                            as variant,
    pone.parentid                          as entity_id,
    'Parents'                              as entity_type,
    min(pop.entered_at)                    as entered_at
  from parent_one_child pone
    join cache.graph on graph.parentid = pone.parentid
    join ab_platform.experiment_populations_android_student_routines_student_population pop
      on pop.pstudentid = graph.pstudentid
  group by 1, 2, 3, 4
);

begin;
grant all on ab_platform.experiment_populations_android_student_routines_to_parents_v3_temp to group team;
grant all on ab_platform.experiment_populations_android_student_routines_to_parents_v3_temp to astronomer;
drop table if exists ab_platform.experiment_populations_android_student_routines_to_parents_v3 cascade;
alter table ab_platform.experiment_populations_android_student_routines_to_parents_v3_temp
  rename to experiment_populations_android_student_routines_to_parents_v3;

