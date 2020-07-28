drop table if exists ab_platform.experiment_populations_classroom_tab_comms_oriented cascade;

create table ab_platform.experiment_populations_classroom_tab_comms_oriented_temp distkey(entity_id) sortkey(entity_id) as (
  select
    '0bad1983-344c-452d-a911-91bb44d986f4' as experiment_id,
    min(eventvalue)          as variant,
    entityid as entity_id,
    'Teachers' as entity_type,
    min(createdat)          as entered_at
  FROM logs.classroom_tab_comms_experiment_event events
  WHERE
    /* REPLACE THIS WITH YOUR EXPERIMENT EXPOSURE EVENT */
    eventname = 'ios.teacher.classroom.comms_experiment.exposure'

    /* REPLACE THIS WITH YOUR EXPERIMENT START AND END DATE */
    AND events.createdat >= '2020-07-26' --Experiment start date
    and events.createdat < '2020-07-26'::timestamp + interval '30 days'
    AND events.eventvalue != 'off'
    AND events.eventvalue is not null
  GROUP BY 
    entityid, 
    experiment_id, 
    entity_type
);

begin;
grant all on ab_platform.experiment_populations_classroom_tab_comms_oriented_temp to group team;
grant all on ab_platform.experiment_populations_classroom_tab_comms_oriented_temp to astronomer;
drop table if exists ab_platform.experiment_populations_classroom_tab_comms_oriented cascade;
alter table ab_platform.experiment_populations_classroom_tab_comms_oriented_temp rename to experiment_populations_classroom_tab_comms_oriented;
commit;
