/* Experiment population for the Share with your school leader-experiment
   Population kind is Teachers
   experiment_id: 45a65009-1a3b-4b5a-ae96-d47432e0c5ad
*/

drop table if exists ab_platform.experiment_populations_45a650091a3b4b5aae96d47432e0c5ad;
create table ab_platform.experiment_populations_45a650091a3b4b5aae96d47432e0c5ad
  distkey(entity_id)
  sortkey(entity_id)
  as (
    WITH activity AS (
      SELECT
        teacherid
      , SUM(CASE WHEN is_3d7_wact AND week BETWEEN DATE('2019-08-01') AND DATE('2020-03-20') THEN 1 ELSE 0 END) AS n_3d7_wact_weeks_since_aug_19
      FROM temp.tiger_team_wact_events_by_week
      GROUP BY teacherid
    )

    , assignment AS (
      SELECT
        firstname AS first_name
      , emailaddress AS email
      , f_to_mongo_id(teacher.teacherid) AS id
      , teacher.teacherid
      , teacher.schoolid
      , CASE
        -- Randomize by first letter of sha, which is 0-9a-f (each is )
        -- ~12% teacher hold out (if teacher hash starts with 0 or 1, 6% each)
          WHEN FUNC_SHA1(CONCAT(COALESCE(schoolid, teacher.teacherid),  'salt - change the number to get a new ordering: share with your SL 3432')) SIMILAR TO '[0-1]%' THEN 'control'
          WHEN FUNC_SHA1(CONCAT(COALESCE(schoolid, teacher.teacherid),  'salt - change the number to get a new ordering: share with your SL 3432')) SIMILAR TO '[2-3]%' THEN 'push only'
          WHEN FUNC_SHA1(CONCAT(COALESCE(schoolid, teacher.teacherid),  'salt - change the number to get a new ordering: share with your SL 3432')) SIMILAR TO '[4-5]%' THEN 'email only'
          WHEN FUNC_SHA1(CONCAT(COALESCE(schoolid, teacher.teacherid),  'salt - change the number to get a new ordering: share with your SL 3432')) SIMILAR TO '[6-9a-f]%' THEN 'email and push'
          ELSE 'impossible'
        END AS variant
      FROM cache.teacher
      LEFT JOIN activity
        ON teacher.teacherid = activity.teacherid
      WHERE TRUE
        AND locale LIKE 'en-%'
        AND marketingemailoptout is FALSE
        AND is_tester is false
        AND (ismentor OR n_3d7_wact_weeks_since_aug_19 >= 20)
     )

     , assignment_by_school AS (
        SELECT DISTINCT
          variant
        , schoolid
        FROM assignment
        WHERE schoolid IS NOT NULL
    )

    SELECT DISTINCT
      '45a65009-1a3b-4b5a-ae96-d47432e0c5ad' AS experiment_id
    , assignment_by_school.variant AS variant
    , teacher.teacherid AS entity_id
    , 'Teachers' AS entity_type
    , '2020-03-20'::TIMESTAMP AS entered_at
    , 'manual' AS origin
    FROM assignment_by_school
    JOIN cache.teacher AS teacher
      ON assignment_by_school.schoolid = teacher.schoolid
);

grant all on ab_platform.experiment_populations_45a650091a3b4b5aae96d47432e0c5ad to group team;


