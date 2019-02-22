# -*- coding: utf-8 -*-
from setuptools import setup

setup(
    # Basic info
    name='docker-airflow',
    version='1.0.0',
    author='Calm.com, Inc.',
    author_email='kamla@calm.com',
    url='https://github.com/calm/docker-airflow',
    description='Runs Airflow DAGs at Calm',
    long_description=open('README.md').read(),
    packages=[
        'calm_logger',
        'dags',
        'operators',
        'templates',
        'test',
        'test.operators',
    ],
    install_requires=[],
    package_data={
        'templates.sql': ['*.sql'],
        'templates.sql.sync_bi_tables': ['*.sql'],
        'test.ge_validations': ['*.json'],
    },
    # Other configurations
    zip_safe=False,
    platforms='any',
)
