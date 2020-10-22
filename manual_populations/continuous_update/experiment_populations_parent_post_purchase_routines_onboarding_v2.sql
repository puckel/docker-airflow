drop table if exists ab_platform.experiment_populations_parent_post_purchase_routines_onboarding_v2_temp cascade;

create table ab_platform.experiment_populations_parent_post_purchase_routines_onboarding_v2_temp
  distkey (entity_id)
  sortkey (entity_id) as (
  select
    '4cc949f9-fd5a-460f-a69c-9f640ef7ebfd'                                       as experiment_id,
    eventvalue as variant,
    entityid                                                                     as entity_id,
    'Parents'                                                                    as entity_type,
    min(createdat)                                                               as entered_at
  from logs.product_event_no_pii
  where
    eventname = 'ios.experiment.iOS_parentRoutinesSignupOnboardingEnabled'
    and createdat >= '2020-10-14'
    and createdat <= getdate()
    and eventvalue != 'off'
    and appversion = '7.11.0'
  group by
    entity_id,
    variant,
    experiment_id,
    entity_type
);

begin;
grant all on ab_platform.experiment_populations_parent_post_purchase_routines_onboarding_v2_temp to group team;
grant all on ab_platform.experiment_populations_parent_post_purchase_routines_onboarding_v2_temp to astronomer;
drop table if exists ab_platform.experiment_populations_parent_post_purchase_routines_onboarding_v2 cascade;
alter table ab_platform.experiment_populations_parent_post_purchase_routines_onboarding_v2_temp
  rename to experiment_populations_parent_post_purchase_routines_onboarding_v2;
commit;
