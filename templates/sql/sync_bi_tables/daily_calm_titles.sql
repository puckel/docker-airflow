/*
This table allows you to know which daily calm title/guide_variant_id was played on each day since inception of DC on 2016-05-02.
This table is needed because titles of daily calms correspond to a position in guide_variants but not to a date.
So only know that a daily calm was played on a given date if you know that position 1 started on 2016-05-02
and that there has been 1 per day since.

NOTE - postgres generate_series doesnt work with tables in redshift so below painful RS roundabout way*/
DROP TABLE IF EXISTS {{ params.schema }}.daily_calm_titles_stage;
CREATE TABLE {{ params.schema }}.daily_calm_titles_stage 
as
with digit as (
    select 0 as d union all
    select 1 union all select 2 union all select 3 union all
    select 4 union all select 5 union all select 6 union all
    select 7 union all select 8 union all select 9
),
seq as (
    select a.d + (10 * b.d) + (100 * c.d) + (1000 * d.d) as num
    from digit a
        cross join
        digit b
        cross join
        digit c
        cross join
        digit d
    order by 1
),
generated_series as (
select distinct (getdate()::date - seq.num)::date as "played_date",
row_number() over (order by played_date) as position
from seq
where (getdate()::date - seq.num)::date > '2016-05-01'
order by played_date
)
SELECT generated_series.played_date,
  gv.position,
  gv.id AS guide_variant_id,
  gv.title,
  current_timestamp as table_created_at
FROM generated_series
RIGHT JOIN (SELECT position,
        gv.id,
        gv.title
    FROM appdb.guide_variants gv
    JOIN appdb.guides g on g.id = gv.guide_id
    WHERE g.program_variant_id = 'Wd3y3az6Q'
    order by gv.position
    ) gv
ON generated_series.position = gv.position
where generated_series.played_date is not null
order by generated_series.played_date
