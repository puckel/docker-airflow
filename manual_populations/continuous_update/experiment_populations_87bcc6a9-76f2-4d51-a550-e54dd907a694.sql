/* Experiment population for the Default Goal-experiment
   experiment_id: 87bcc6a9-76f2-4d51-a550-e54dd907a694

   We restrict to parents whose free trial did not start more than
   4 days before first exposure to experiment.
*/

drop table if exists ab_platform.experiment_populations_87bcc6a976f24d51a550e54dd907a694;
create table ab_platform.experiment_populations_87bcc6a976f24d51a550e54dd907a694
  distkey(entity_id)
  sortkey(entity_id)
  as (
  WITH population_raw AS (
      SELECT
        '87bcc6a9-76f2-4d51-a550-e54dd907a694' AS experiment_id
      , events.eventvalue AS variant
      , events.entityid AS entity_id
      , 'Parents' AS entity_type
      , MIN(free_trial.trialstart) AS trial_start_at
      , MIN(events.createdat) AS entered_at
      , 'manual' AS origin
      FROM frog.events AS events
      JOIN frog.parent_paid_product_free_trial_ios AS free_trial
        ON free_trial.parentid = events.entityid
      WHERE events.eventname = 'api.experiment.defaultGoal'
        AND events.eventday BETWEEN '2020-01-05'::DATE AND '2020-01-05'::DATE + INTERVAL '21 days'
        AND events.eventvalue != 'off'
        AND events.eventvalue IS NOT NULL
      GROUP BY 1,2,3,4,7
  )

  SELECT
    experiment_id
  , variant
  , entity_id
  , entity_type
  , entered_at
  , origin
  FROM population_raw
  WHERE trial_start_at >= entered_at - INTERVAL '4 days'
);

grant all on ab_platform.experiment_populations_87bcc6a976f24d51a550e54dd907a694 to group team;


