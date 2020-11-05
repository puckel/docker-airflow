/* Marketing campaign for active parents
   Population is parents
   Experiment id is 821192b5-4a3e-4c7c-a3d6-0cde36327f7f
 */

DROP TABLE if exists ab_platform.experiment_populations_alicanto_marketing_camp_active_parents_temp cascade;

create table ab_platform.experiment_populations_alicanto_marketing_camp_active_parents_temp
  distkey (entity_id)
  sortkey (entity_id) as (
  select
    '821192b5-4a3e-4c7c-a3d6-0cde36327f7f' as experiment_id,
    variant as variant,
    parentid as entity_id,
    'Parents' as entity_type,
    date('2020-10-23') as entered_at
  from temp.alicanto_marketing_camp_oct_2020_active_parents_test_groups_bucketed_test_control_only
);

begin;
grant all on ab_platform.experiment_populations_alicanto_marketing_camp_active_parents_temp to group team;
grant all on ab_platform.experiment_populations_alicanto_marketing_camp_active_parents_temp to astronomer;
drop table if exists ab_platform.experiment_populations_alicanto_marketing_camp_active_parents cascade;
alter table ab_platform.experiment_populations_alicanto_marketing_camp_active_parents_temp
  rename to experiment_populations_alicanto_marketing_camp_active_parents
commit;
