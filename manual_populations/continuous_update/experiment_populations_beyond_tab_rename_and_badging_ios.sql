/* Ben's hello world population
*/

drop table if exists ab_platform.experiment_populations_beyond_tab_rename_and_badging_ios;
create table ab_platform.experiment_populations_beyond_tab_rename_and_badging_ios
  distkey(entity_id)
  sortkey(entity_id)
  as (
  SELECT
    '18aed289-31e4-4a2a-9125-09806c27dcfa' AS experiment_id
  , eventvalue as variant
  , entityid AS entity_id
  , 'Parents' AS entity_type
  , min(createdat) AS entered_at
  from logs.product_event_no_pii
  where 
    eventname = 'ios.experiment.iOS_parentBeyondExperienceExperiment'
    and eventvalue != 'off'
    and createdat >= '2020-09-03'
  GROUP BY 1,2,3,4
);

grant all on ab_platform.experiment_populations_beyond_tab_rename_and_badging_ios to group team;
grant all on ab_platform.experiment_populations_beyond_tab_rename_and_badging_ios to astronomer;
