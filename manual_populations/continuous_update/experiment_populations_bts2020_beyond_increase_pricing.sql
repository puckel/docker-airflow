drop table if exists ab_platform.experiment_populations_bts2020_beyond_increase_pricing_temp cascade;

create table ab_platform.experiment_populations_bts2020_beyond_increase_pricing_temp
  distkey (entity_id)
  sortkey (entity_id) as (
  select
    'e05644c0-869c-4527-99a8-b3b584abaf51'                                       as experiment_id,
    json_extract_path_text(metadata, 'iOS_parentPricingExperimentBTS2020', TRUE) as variant,
    entityid                                                                     as entity_id,
    'Parents'                                                                    as entity_type,
    min(createdat)                                                               as entered_at
  from logs.product_event_no_pii
  where
    eventname = 'ios.parent.paid_product.sales_page.exposure'
    and createdat >= '2020-09-02'
    and createdat <= getdate()
    and json_extract_path_text(metadata, 'iOS_parentPricingExperimentBTS2020', TRUE) != 'off'
    and json_extract_path_text(metadata, 'iOS_parentPricingExperimentBTS2020', TRUE) != ''
  group by
    entity_id,
    variant,
    experiment_id,
    entity_type
);

begin;
grant all on ab_platform.experiment_populations_bts2020_beyond_increase_pricing_temp to group team;
grant all on ab_platform.experiment_populations_bts2020_beyond_increase_pricing_temp to astronomer;
drop table if exists ab_platform.experiment_populations_bts2020_beyond_increase_pricing cascade;
alter table ab_platform.experiment_populations_bts2020_beyond_increase_pricing_temp
  rename to experiment_populations_bts2020_beyond_increase_pricing;
commit;
