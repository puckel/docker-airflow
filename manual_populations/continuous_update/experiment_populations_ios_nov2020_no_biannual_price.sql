drop table if exists ab_platform.experiment_populations_ios_nov2020_no_biannual_price_temp;
create table ab_platform.experiment_populations_ios_nov2020_no_biannual_price_temp
  distkey (entity_id)
  sortkey (entity_id) as (
  select
    '1b991ced-c0a3-4d18-9020-b8fea6115a90'                                       as experiment_id,
    json_extract_path_text(metadata, 'iOS_nov2020noBiAnnualPriceExperiment', TRUE) as variant,
    entityid                                                                     as entity_id,
    'Parents'                                                                    as entity_type,
    min(createdat)                                                               as entered_at
  from logs.product_event_no_pii
  where
    eventname = 'ios.parent.paid_product.sales_page.exposure'
    and json_extract_path_text(metadata, 'iOS_nov2020noBiAnnualPriceExperiment', TRUE) is not NULL
    and json_extract_path_text(metadata, 'iOS_nov2020noBiAnnualPriceExperiment', TRUE) is not ''
    and createdat >= '2020-11-12'
    and createdat <= getdate()
  group by
    entity_id,
    variant,
    experiment_id,
    entity_type
);

begin;
grant all on ab_platform.experiment_populations_ios_nov2020_no_biannual_price_temp to group team;
grant all on ab_platform.experiment_populations_ios_nov2020_no_biannual_price_temp to astronomer;
drop table if exists ab_platform.experiment_populations_ios_nov2020_no_biannual_price cascade;
alter table ab_platform.experiment_populations_ios_nov2020_no_biannual_price_temp
  rename to experiment_populations_ios_nov2020_no_biannual_price;
commit;
