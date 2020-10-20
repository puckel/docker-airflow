/* Experiment population for the Value based onboarding-experiment

   Same as Value Based onboard but here we are only looking at the Teachers
   that got exposed before schools closed, i.e. June 1.

   experiment_id: 609d25ab-b709-46a2-b4f9-219b8a2b0e86
*/

drop table if exists ab_platform.experiment_populations_609d25abb70946a2b4f9219b8a2b0e86;
create table ab_platform.experiment_populations_609d25abb70946a2b4f9219b8a2b0e86
  distkey(entity_id)
  sortkey(entity_id)
  as (
  SELECT
    '609d25ab-b709-46a2-b4f9-219b8a2b0e86' AS experiment_id
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
    AND events.createdat BETWEEN '2020-05-19'::DATE AND '2020-05-31'::DATE
    AND teachers.createdat BETWEEN '2020-05-19'::DATE AND '2020-05-31'::DATE
  GROUP BY 1,2,3,4,6
);

grant all on ab_platform.experiment_populations_609d25abb70946a2b4f9219b8a2b0e86 to group team;
grant all on ab_platform.experiment_populations_609d25abb70946a2b4f9219b8a2b0e86 to astronomer;
