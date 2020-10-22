drop table if exists ab_platform.experiment_populations_parent_post_signup_routines_onboarding_temp cascade;

create table ab_platform.experiment_populations_parent_post_signup_routines_onboarding_temp
  distkey (entity_id)
  sortkey (entity_id) as (
  select
    '398efd1a-caa2-4bfa-96c5-c1e0006d91bd'                                       as experiment_id,
    eventvalue as variant,
    entityid                                                                     as entity_id,
    'Parents'                                                                    as entity_type,
    min(createdat)                                                               as entered_at
  from logs.product_event_no_pii
  where
    eventname = 'ios.experiment.iOS_parentRoutinesSignupOnboardingEnabled'
    and createdat >= '2020-09-09'
    and createdat <= getdate()
    and eventvalue != 'off'
  group by
    entity_id,
    variant,
    experiment_id,
    entity_type
);

begin;
grant all on ab_platform.experiment_populations_parent_post_signup_routines_onboarding_temp to group team;
grant all on ab_platform.experiment_populations_parent_post_signup_routines_onboarding_temp to astronomer;
drop table if exists ab_platform.experiment_populations_parent_post_signup_routines_onboarding cascade;
alter table ab_platform.experiment_populations_parent_post_signup_routines_onboarding_temp
  rename to experiment_populations_parent_post_signup_routines_onboarding;
commit;
