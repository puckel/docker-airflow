frog.beyond_modal_and_banner_ios_experiment_audience

drop table if exists ab_platform.experiment_populations_sep2020_routines_upsell_temp cascade;

create table ab_platform.experiment_populations_sep2020_routines_upsell_temp
  distkey (entity_id)
  sortkey (entity_id) as (
  select
    '0ea3808d-5211-46f2-a342-6e18c5538f4a'                                       as experiment_id,
    variant,
    entityid                                                                     as entity_id,
    'Parents'                                                                    as entity_type,
    first_exposure                                                               as entered_at
  from frog.beyond_modal_and_banner_ios_experiment_audience
);

begin;
grant all on ab_platform.experiment_populations_sep2020_routines_upsell_temp to group team;
grant all on ab_platform.experiment_populations_sep2020_routines_upsell_temp to astronomer;
drop table if exists ab_platform.experiment_populations_sep2020_routines_upsell cascade;
alter table ab_platform.experiment_populations_sep2020_routines_upsell_temp
  rename to experiment_populations_sep2020_routines_upsell;
commit;
