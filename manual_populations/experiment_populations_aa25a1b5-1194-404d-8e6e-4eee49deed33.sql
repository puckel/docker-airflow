/* Experiment population for the Summer 2020 sales page messaging-experiment
   experiment_id: aa25a1b5-1194-404d-8e6e-4eee49deed33
*/

drop table if exists ab_platform.experiment_populations_aa25a1b51194404d8e6e4eee49deed33;
create table ab_platform.experiment_populations_aa25a1b51194404d8e6e4eee49deed33
  distkey(entity_id)
  sortkey(entity_id)
  as (

  SELECT
    'aa25a1b5-1194-404d-8e6e-4eee49deed33' AS experiment_id
  , eventvalue AS variant
  , entityid AS entity_id
  , 'Parents' AS entity_type
  , MIN(createdat) AS entered_at
  , 'manual' AS origin
  FROM frog.events events
  WHERE eventname = 'ios.parent.paid_product.sales_page.exposure'
    AND createdat BETWEEN '2020-06-25'::TIMESTAMP AND GETDATE() - INTERVAL '1 day'
    AND NVL(eventvalue, '') IN ('copy1', 'control')
  GROUP BY 1,2,3,4,6
);

grant all on ab_platform.experiment_populations_aa25a1b51194404d8e6e4eee49deed33 to group team;


