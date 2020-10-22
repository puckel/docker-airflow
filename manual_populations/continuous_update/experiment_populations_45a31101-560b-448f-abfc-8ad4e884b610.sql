/* Experiment population for the Value based onboarding-experiment
   experiment_id: 45a31101-560b-448f-abfc-8ad4e884b610
*/

drop table if exists ab_platform.experiment_populations_45a31101560b448fabfc8ad4e884b610;
create table ab_platform.experiment_populations_45a31101560b448fabfc8ad4e884b610
  distkey(entity_id)
  sortkey(entity_id)
  as (
  SELECT
    '45a31101-560b-448f-abfc-8ad4e884b610' AS experiment_id
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
    AND events.createdat BETWEEN '2020-05-19'::DATE AND '2020-05-19'::DATE + INTERVAL '56 days'
    AND teachers.createdat BETWEEN '2020-05-19'::DATE AND '2020-05-19'::DATE + INTERVAL '56 days'
  GROUP BY 1,2,3,4,6
);

grant all on ab_platform.experiment_populations_45a31101560b448fabfc8ad4e884b610 to group team;
grant all on ab_platform.experiment_populations_45a31101560b448fabfc8ad4e884b610 to astronomer;
