drop table if exists ab_platform.experiment_populations_bts2020_beyond_increase_pricing_v2_temp cascade;

create table ab_platform.experiment_populations_bts2020_beyond_increase_pricing_v2_temp
  distkey (entity_id)
  sortkey (entity_id) as (
  select
    'b05d97c9-e744-4c67-81b1-8aa6c9d5ce34'                                         as experiment_id,
    json_extract_path_text(metadata, 'iOS_parentPricingExperimentBTS2020v2', TRUE) as variant,
    entityid                                                                       as entity_id,
    'Parents'                                                                      as entity_type,
    min(createdat)                                                                 as entered_at
  from logs.product_event_no_pii
  where
    eventname = 'ios.parent.paid_product.sales_page.exposure'
    and createdat >= '2020-10-13'
    and createdat < '2020-11-14 00:00:00'
    and json_extract_path_text(metadata, 'iOS_parentPricingExperimentBTS2020v2', TRUE) != 'off'
    and json_extract_path_text(metadata, 'iOS_parentPricingExperimentBTS2020v2', TRUE) != ''
  group by
    entity_id,
    variant,
    experiment_id,
    entity_type
);

begin;
grant all on ab_platform.experiment_populations_bts2020_beyond_increase_pricing_v2_temp to group team;
grant all on ab_platform.experiment_populations_bts2020_beyond_increase_pricing_v2_temp to astronomer;
drop table if exists ab_platform.experiment_populations_bts2020_beyond_increase_pricing_v2 cascade;
alter table ab_platform.experiment_populations_bts2020_beyond_increase_pricing_v2_temp
  rename to experiment_populations_bts2020_beyond_increase_pricing_v2;
commit;
