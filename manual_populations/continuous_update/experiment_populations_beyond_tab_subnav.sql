drop table if exists ab_platform.experiment_populations_beyond_tab_subnav cascade;

create table ab_platform.experiment_populations_beyond_tab_subnav_temp distkey(entity_id) sortkey(entity_id) as (
  select
    '683f8d77-c3c8-46fc-a136-dbd56dd93c1d' as experiment_id,
    eventvalue          as variant,
    entityid as entity_id,
    'Parents' as entity_type,
    min(date_trunc('day', createdat))          as entered_at
  FROM logs.product_event_no_pii events
  WHERE
    /* REPLACE THIS WITH YOUR EXPERIMENT EXPOSURE EVENT */
    eventname = 'ios.experiment.iOS_parentActivitiesTabSubnav'

    /* REPLACE THIS WITH YOUR EXPERIMENT START AND END DATE */
    AND events.createdat >= '2020-09-23' --Experiment start date
    and events.createdat < '2020-09-23'::timestamp + interval '60 days'
    AND events.eventvalue != 'off'
    AND events.eventvalue is not null
  GROUP BY
    1,2,3,4
);

begin;
grant all on ab_platform.experiment_populations_beyond_tab_subnav_temp to group team;
grant all on ab_platform.experiment_populations_beyond_tab_subnav_temp to astronomer;
drop table if exists ab_platform.experiment_populations_beyond_tab_subnav cascade;
alter table ab_platform.experiment_populations_beyond_tab_subnav_temp rename to experiment_populations_beyond_tab_subnav;
commit;
