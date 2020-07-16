from airflow.hooks import PostgresHook

from scipy.stats import ttest_ind_from_stats
from statsmodels.stats.proportion import proportions_chisquare

import os
import pandas
import sqlalchemy


CONTROL_PANEL_TABLE = 'ab_platform.experiment_control_panel'
POPULATION_MAPPING_TABLE = 'ab_platform.experiment_to_population_map'
EXPERIMENT_INTERMEDIATE_RESULTS_TABLE = 'ab_platform.experiment_results_intermediate'
EXPERIMENT_INTERMEDIATE_RESULTS_TABLE_ONLY = 'experiment_results_intermediate'


def get_active_experiment_and_population_map(conn_id, target_date):
    pg_hook = PostgresHook(conn_id)
    query = '''
    SELECT
        a.experiment_id,
        a.population_kind,
        b.table_name
    FROM %s a JOIN %s b ON a.experiment_id = b.experiment_id
    WHERE '%s' >= start_date and archived = false;
    ''' % (CONTROL_PANEL_TABLE, POPULATION_MAPPING_TABLE, target_date.isoformat())
    records = pg_hook.get_records(query)
    experiment_to_population_map = {}
    for experiment_id, population_kind, table_name in records:
        experiment_to_population_map[experiment_id] = {
            'population_table_name': table_name,
            'population_schema_name':  'ab_platform',
            'population_type': population_kind,
        }
    return experiment_to_population_map


def create_intermediate_results_table(conn_id):
    pg_hook = PostgresHook(conn_id)
    query = '''
    create table if not exists {}
    (
        experiment_id varchar(36) not null,
        variant varchar(128) not null,
        metric_name varchar(128) not null,
        metric_type varchar(128) not null,
        segment varchar(128) not null,
        day timestamp not null,
        numerator integer,
        denominator integer,
        mean double precision,
        standard_deviation double precision,
        primary key(experiment_id, variant, metric_name, metric_type, segment, day)
    );
    '''.format(EXPERIMENT_INTERMEDIATE_RESULTS_TABLE)
    pg_hook.run(query)


def get_metric_names(population_type):
    parent_dir = os.path.join('./queries', population_type.lower())
    l = []
    for item in os.listdir(parent_dir):
        metric_name = item.split('.')[0]
        l.append(metric_name)

    return l


def get_templates(population_type):
    d = {}
    parent_dir = os.path.join('./queries', population_type.lower())
    for item in os.listdir(parent_dir):
        item_path = os.path.join(parent_dir, item)
        metric_name = item.split('.')[0]
        with open(item_path, 'r') as f:
            s = f.read()
            d[metric_name] = s

    return d


def calculate_intermediate_result_for_day(conn_id, dt, experiment_to_population_map):
    pg_hook = PostgresHook(conn_id)
    # Get metric templates from ../queries
    # This is basically a query cache so we don't have to constantly open and close files
    population_types = set([population_metadata['population_type']
                            for population_metadata in experiment_to_population_map.values()])

    population_templates = {}
    for population_type in population_types:
        d = get_templates(population_type)
        population_templates[population_type] = d

    # Fill out the template with variables and get the records
    # Inserts happen in the next step.
    all_records = []
    for experiment_id, population_metadata in experiment_to_population_map.items():
        print('Getting the intermediate results for {}'.format(experiment_id))
        template_map = population_templates.get(
            population_metadata['population_type'], {})
        for metric_name, template in template_map.items():
            print(metric_name)
            s = template % {
                'metric_name': metric_name,
                'table_name': '%s.%s' % (population_metadata['population_schema_name'],
                                         population_metadata['population_table_name']),
                'ds': dt.isoformat(),
            }
            records = pg_hook.get_records(s)
            all_records += records

    return all_records


def insert_intermediate_records(conn_id, records):
    pg_hook = PostgresHook(conn_id)

    # We have to worry about double inserts if it runs on the same day
    # So we delete records from the table with the same experiment_id, metric_name, and date
    for experiment_id, variant, metric_name, metric_type, \
            segment, day, numerator, denominator, mean, \
            standard_deviation in records:

        query = '''
            begin;
            DELETE FROM %(table_name)s
            WHERE
                experiment_id = '%(experiment_id)s' and
                variant = '%(variant)s' and
                metric_name = '%(metric_name)s' and
                metric_type = '%(metric_type)s' and
                segment = '%(segment)s' and
                day = '%(day)s';

            INSERT INTO %(table_name)s
            (experiment_id, variant, metric_name, metric_type, segment, day, numerator, denominator, mean, standard_deviation) VALUES
            ('%(experiment_id)s', '%(variant)s', '%(metric_name)s', '%(metric_type)s', '%(segment)s', '%(day)s', %(numerator)d, %(denominator)d, %(mean)f, %(standard_deviation)f);
            commit;
            ''' % {
            'table_name': EXPERIMENT_INTERMEDIATE_RESULTS_TABLE,
            'experiment_id': experiment_id,
            'variant':  variant,
            'metric_name': metric_name,
            'metric_type': metric_type,
            'segment': segment,
            'day': day.date().isoformat(),
            'numerator': numerator or 0,
            'denominator': denominator or 0,
            'mean': mean or 0,
            'standard_deviation': standard_deviation or 0,
        }
        pg_hook.run(query)


def _calculate_p_value(x):
    p_value = None
    if x['variant'] == x['variant_compared']:
        pass

    elif x['metric_type'].lower() == 'ratio':
        try:
            result = ttest_ind_from_stats(
                mean1=x['mean'],
                std1=x['standard_deviation'],
                nobs1=x['denominator'],
                mean2=x['mean_compared'],
                std2=x['standard_deviation_compared'],
                nobs2=x['denominator_compared']
            )
            p_value = result.pvalue
        except ZeroDivisionError as err:
            print("Got an ZeroDivisionError %s" % err)
            print("Row is %s" % x)
            print("Setting p_value to None")
            p_value = None  # Shouldn't really ever get here, but just in case

    elif x['metric_type'].lower() == 'proportional':
        numerators = (x['numerator'], x['numerator_compared'])
        denominators = (x['denominator'], x['denominator_compared'])
        try:
            result = proportions_chisquare(numerators, denominators)
            p_value = result[1]
        except ZeroDivisionError:
            print("Got an ZeroDivisionError %s" % err)
            print("Row is %s" % x)
            print("Setting p_value to None")
            p_value = None  # Shouldn't really ever get here, but just in case

    return p_value


def calculate_results(conn_id):
    # Take the whole table and replace
    # Naturally idempotent, we can use the pandas helper methods
    pg_hook = PostgresHook(conn_id)

    results_intermediate = pandas.read_sql_table(EXPERIMENT_INTERMEDIATE_RESULTS_TABLE_ONLY,
                                                 pg_hook.get_sqlalchemy_engine(), schema='ab_platform')

    print("Found {} intermediate results in intermediate table".format(
        len(results_intermediate)))
    results_intermediate.set_index(
        ['experiment_id', 'metric_name', 'metric_type', 'segment', 'day'], inplace=True)

    results_expanded = results_intermediate.join(
        results_intermediate, rsuffix='_compared').reset_index()
    results_expanded['p_value'] = results_expanded.apply(
        _calculate_p_value, axis=1)

    print("Writing {} results".format(len(results_expanded)))

    results_expanded.to_sql(
        name='experiment_results',
        con=pg_hook.get_sqlalchemy_engine(),
        schema='ab_platform',
        if_exists='replace',
        index=False,
        dtype={
            "experiment_id": sqlalchemy.types.VARCHAR(36),
            "variant": sqlalchemy.types.VARCHAR(128),
            "metric_name": sqlalchemy.types.VARCHAR(128),
            "metric_type": sqlalchemy.types.VARCHAR(128),
            "segment": sqlalchemy.types.VARCHAR(128),
            "day": sqlalchemy.types.DateTime,
            "numerator": sqlalchemy.types.Integer,
            "denominator": sqlalchemy.types.Integer,
            "mean": sqlalchemy.types.Float,
            "standard_deviation": sqlalchemy.types.Float,
            "p_value": sqlalchemy.types.Float,
            "variant_compared": sqlalchemy.types.VARCHAR(128),
            "numerator_compared": sqlalchemy.types.Integer,
            "denominator_compared": sqlalchemy.types.Integer,
            "mean_compared": sqlalchemy.types.Float,
            "standard_deviation_compared": sqlalchemy.types.Float,
        }
    )
