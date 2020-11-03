/* Marketing campaign to parents that receive or give more than 15 points in last 5 days
   Population kind is Parents
   experiment_id: fd60121c-172b-40e2-9e02-94a1420ec273
*/

drop table if exists ab_platform.experiment_populations_alicanto_marketing_camp_points_receivers_temp cascade;

create table ab_platform.experiment_populations_alicanto_marketing_camp_points_receivers_temp
  distkey (entity_id)
  sortkey (entity_id) as (
  select
    'fd60121c-172b-40e2-9e02-94a1420ec273' as experiment_id,
    variant as variant,
    entity_id as entity_id,
    'Parents' as entity_type,
    date('2020-10-23') as entered_at
  from temp.alicanto_marketing_camp_oct_2020_points_receivers_test_groups_bucketed_test_control_only
);

begin;
grant all on ab_platform.experiment_populations_alicanto_marketing_camp_points_receivers_temp to group team;
grant all on ab_platform.experiment_populations_alicanto_marketing_camp_points_receivers_temp to astronomer;
drop table if exists ab_platform.experiment_populations_alicanto_marketing_camp_points_receivers cascade;
alter table ab_platform.experiment_populations_alicanto_marketing_camp_points_receivers_temp
  rename to experiment_populations_alicanto_marketing_camp_points_receivers;
commit;
