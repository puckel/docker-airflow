/* Experiment population for the Value based onboarding-experiment

   Same as Value Based onboard but here we are only looking at the Teachers
   that got exposed after schools started opening againg, i.e. June 30..

   experiment_id: 0599f61d-e2a7-4131-b3d0-be337de98627
*/

drop table if exists ab_platform.experiment_populations_0599f61de2a74131b3d0be337de98627;
create table ab_platform.experiment_populations_0599f61de2a74131b3d0be337de98627
  distkey(entity_id)
  sortkey(entity_id)
  as (
  SELECT
    '0599f61d-e2a7-4131-b3d0-be337de98627' AS experiment_id
  , CASE
      WHEN events.eventname = 'launchpad.new_user_communication_nux.seen.onboardingValues'
      THEN 'experiment'
      ELSE 'control'
    END AS variant
  , events.entityid AS entity_id
  , 'Teachers' AS entity_type
  , MIN(events.createdat) AS entered_at
  , 'manual' AS origin
  FROM logs.product_event_last_6_months AS events
  JOIN cache.teacher AS teachers
    ON teachers.teacherid = events.entityid
  WHERE events.eventname IN ('launchpad.new_user_communication_nux.seen.onboardingValues', 'launchpad.new_user_communication_nux.seen.welcomeMessage')
    AND events.createdat >= '2020-07-01'::DATE
    AND teachers.createdat >= '2020-07-01'::DATE
  GROUP BY 1,2,3,4,6
);

grant all on ab_platform.experiment_populations_0599f61de2a74131b3d0be337de98627 to group team;
