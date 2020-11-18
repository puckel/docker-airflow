import pandas as pd
import sqlalchemy

from airflow.hooks import PostgresHook


EXPERIMENT_SWITCHED_POPULATIONS_TABLE = 'ab_platform.experiment_populations_with_switched_users'


def identify_switched_population(conn_id, experiment_to_population_map):
    pg_hook = PostgresHook(conn_id)

    # Create one large query that looks at each experiment_population-table, picks entity_ids with
    # multiple entries, i,e. have switched between variants, and the stitch all of them together
    # into one table

    query_start = """drop table if exists {table};
    create table {table}
        distkey(experiment_id)
        sortkey(experiment_id)
        as (
    """.format(table=EXPERIMENT_SWITCHED_POPULATIONS_TABLE)
    query_end = """order by entity_id, entered_at
    );
    grant all on {table} to group team;
    grant all on {table} to astronomer;    
    """.format(table=EXPERIMENT_SWITCHED_POPULATIONS_TABLE)

    query_template = """
    select
        experiment_id
    ,   variant
    ,   entity_id
    ,   entity_type
    ,   entered_at 
    from {population_schema_name}.{population_table_name} 
    join (
        select
            entity_id
        from {population_schema_name}.{population_table_name}
        group by 1
        having count(1) > 1
    ) using(entity_id)
    """
    templated_queries = [query_template.format(population_schema_name=conf['population_schema_name'],
                                               population_table_name=conf['population_table_name'])
                         for experiment_id, conf in experiment_to_population_map.items()]
    union_all_string = """
    union all
    """
    templated_queries_w_union_all = union_all_string.join(templated_queries)
    query = query_start + templated_queries_w_union_all + query_end
    print("Running query...")

    try:
        pg_hook.run(query)
        print("Success!")
    except Exception as e:
        print("Error: {}".format(e))
        raise


def summarize_switched_population(conn_id):
    pg_hook = PostgresHook(conn_id)
    query = """
    select
        experiment_id
    ,   count(distinct entity_id) as switched_variant
    from {table}
    group by 1
    """.format(table=EXPERIMENT_SWITCHED_POPULATIONS_TABLE)
    return pd.read_sql(query, pg_hook.get_sqlalchemy_engine(), index_col='experiment_id')


def summarize_population_check(conn_id, switched_population):
    pg_hook = PostgresHook(conn_id)

    results = switched_population

    results.to_sql(
        name='population_checks',
        con=pg_hook.get_sqlalchemy_engine(),
        schema='ab_platform',
        if_exists='replace',
        index=False,
        dtype={
            "experiment_id": sqlalchemy.types.VARCHAR(36),
            "switched_variant": sqlalchemy.types.Integer
        }
    )