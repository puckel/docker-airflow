drop table if exists ab_platform.experiment_populations_classroom_tab_comms_oriented_v2_temp cascade;

create table ab_platform.experiment_populations_classroom_tab_comms_oriented_v2_temp distkey(entity_id) sortkey(entity_id) as (
  select
    'c18c2d94-7017-43a8-871c-27da330b4942' as experiment_id,
    min(eventvalue) as variant,
    entityid as entity_id,
    'Teachers' as entity_type,
    min(createdat) as entered_at
  FROM logs.classroom_tab_comms_experiment_event events
  WHERE
    /* REPLACE THIS WITH YOUR EXPERIMENT EXPOSURE EVENT */
    eventname = 'ios.teacher.classroom.comms_experiment.exposure'

    /* REPLACE THIS WITH YOUR EXPERIMENT START AND END DATE */
    AND events.createdat >= '2020-09-15' --Experiment start date
    and events.createdat < '2020-09-15'::timestamp + interval '45 days'
    AND events.eventvalue != 'off'
    AND events.eventvalue != 'test'
    AND events.eventvalue != 'control'
    AND events.eventvalue is not null
  GROUP BY 
    entityid, 
    experiment_id, 
    entity_type
);

begin;
grant all on ab_platform.experiment_populations_classroom_tab_comms_oriented_v2_temp to group team;
grant all on ab_platform.experiment_populations_classroom_tab_comms_oriented_v2_temp to astronomer;
drop table if exists ab_platform.experiment_populations_classroom_tab_comms_oriented_v2 cascade;
alter table ab_platform.experiment_populations_classroom_tab_comms_oriented_v2_temp rename to experiment_populations_classroom_tab_comms_oriented_v2;
commit;
