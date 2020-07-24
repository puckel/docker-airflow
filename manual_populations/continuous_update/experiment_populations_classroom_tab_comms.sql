drop table if exists ab_platform.experiment_populations_classroom_tab_comms_temp cascade;

create table ab_platform.experiment_populations_classroom_tab_comms_temp distkey(entity_id) sortkey(entity_id) as (
  select
    'd2836601-4080-45b0-8a1d-3a203d1c4134' as experiment_id,
    min(eventname)          as variant,
    entityid as entity_id,
    'Teachers' as entity_type,
    min(createdat)          as entered_at
  FROM logs.classroom_tab_comms_experiment_event events
  WHERE
    /* REPLACE THIS WITH YOUR EXPERIMENT EXPOSURE EVENT */
    eventname in (
      'teacher.classroom.comms_experiment.control.exposure', 
      'teacher.classroom.comms_experiment.treatment.exposure'
    )

    /* REPLACE THIS WITH YOUR EXPERIMENT START AND END DATE */
    AND events.createdat >= '2020-07-20' --Experiment start date
    and events.createdat < '2020-07-20'::timestamp + interval '30 days'
/*    AND events.eventvalue != 'off'
    AND events.eventvalue is not null */
  GROUP BY 
    entityid, 
    experiment_id, 
    entity_type
);

begin;
grant all on ab_platform.experiment_populations_classroom_tab_comms_temp to group team;
drop table if exists ab_platform.experiment_populations_classroom_tab_comms cascade;
alter table ab_platform.experiment_populations_classroom_tab_comms_temp rename to experiment_populations_classroom_tab_comms;
commit;