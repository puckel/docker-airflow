/* 
In this time of transition, I selected all rows from an experiment
populations table I made in the analytics db, with tweaks to column names:

frog.experiment_populations_bts2020_reconnection_push 
*/

drop table if exists ab_platform.experiment_populations_freemium_limited_vs_weekly cascade;

create table ab_platform.experiment_populations_freemium_limited_vs_weekly_temp distkey(entity_id) sortkey(entity_id) as (
  select
    'bc228ef7-24df-42aa-b365-c9c221ad0385' as experiment_id,
    variant,
    entityid as entity_id,
    'parent' as entity_type,
    first_exposure as entered_at
  FROM frog.freemium_limited_vs_weekly_experiment_audience
);

begin;
grant all on ab_platform.experiment_populations_freemium_limited_vs_weekly_temp to group team;
grant all on ab_platform.experiment_populations_freemium_limited_vs_weekly_temp to astronomer;
drop table if exists ab_platform.experiment_populations_freemium_limited_vs_weekly cascade;
alter table ab_platform.experiment_populations_freemium_limited_vs_weekly_temp rename to experiment_populations_freemium_limited_vs_weekly;
commit;
