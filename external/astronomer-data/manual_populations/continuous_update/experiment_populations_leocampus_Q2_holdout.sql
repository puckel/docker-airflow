/* Ben's hello world population
*/

drop table if exists ab_platform.experiment_populations_leocampus_Q2_holdout;
create table ab_platform.experiment_populations_leocampus_Q2_holdout
  distkey(entity_id)
  sortkey(entity_id)
  as (
  SELECT
    '2454a2d7-1608-4fdc-b43e-5954dd827d9d' AS experiment_id
  , CASE
      WHEN teacher.schoolid LIKE '%A00000000' THEN 'control'
      ELSE 'experiment'
    END AS variant
  , teacher.schoolid AS entity_id
  , 'Schools' AS entity_type
    -- TODO should be greatest of may 1 or school start date
    -- But can't find school start date
  , DATE('2020-05-01') AS entered_at
  FROM cache.teacher AS teacher
  GROUP BY 1,2,3,4
);

grant all on ab_platform.experiment_populations_leocampus_Q2_holdout to group team;
grant all on ab_platform.experiment_populations_leocampus_Q2_holdout to astronomer;
