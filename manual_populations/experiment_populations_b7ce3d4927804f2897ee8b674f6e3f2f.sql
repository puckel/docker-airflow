/* Experiment population for the Value based onboarding-experiment

   Same as Value Based onboard but here we are only looking at the Teachers
   that got exposed during the time when schools were closed, i.e. June 1 to June 30.

   experiment_id: b7ce3d49-2780-4f28-97ee-8b674f6e3f2f
*/

drop table if exists ab_platform.experiment_populations_b7ce3d4927804f2897ee8b674f6e3f2f;
create table ab_platform.experiment_populations_b7ce3d4927804f2897ee8b674f6e3f2f
  distkey(entity_id)
  sortkey(entity_id)
  as (
  SELECT
    'b7ce3d49-2780-4f28-97ee-8b674f6e3f2f' AS experiment_id
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
    AND events.createdat BETWEEN '2020-06-01'::DATE AND '2020-06-30'::DATE
    AND teachers.createdat BETWEEN '2020-06-01'::DATE AND '2020-06-30'::DATE
  GROUP BY 1,2,3,4,6
);

grant all on ab_platform.experiment_populations_b7ce3d4927804f2897ee8b674f6e3f2f to group team;
