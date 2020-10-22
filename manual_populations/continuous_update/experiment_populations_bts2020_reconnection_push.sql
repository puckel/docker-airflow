/* 
This is a weird one where I just selected all rows from an experiment
populations table I made in the analytics db:
frog.experiment_populations_bts2020_reconnection_push 
*/

drop table if exists ab_platform.experiment_populations_bts2020_reconnection_push cascade;

create table ab_platform.experiment_populations_bts2020_reconnection_push_temp distkey(entity_id) sortkey(entity_id) as (
  select
    experiment_id,
    variant,
    entity_id,
    entity_type,
    entered_at
  FROM frog.experiment_populations_bts2020_reconnection_push
);

begin;
grant all on ab_platform.experiment_populations_bts2020_reconnection_push_temp to group team;
grant all on ab_platform.experiment_populations_bts2020_reconnection_push_temp to astronomer;
drop table if exists ab_platform.experiment_populations_bts2020_reconnection_push cascade;
alter table ab_platform.experiment_populations_bts2020_reconnection_push_temp rename to experiment_populations_bts2020_reconnection_push;
commit;
