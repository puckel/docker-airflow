__author__ = 'robertsanders'
__version__ = "1.0.4"

from airflow.models import DagBag, DagModel
from airflow.plugins_manager import AirflowPlugin
from airflow import configuration
from airflow.www.app import csrf

from flask import Blueprint, request, jsonify
from flask_admin import BaseView, expose

from datetime import datetime
import airflow
import logging
import subprocess
import os
import socket

"""
CLIs this REST API exposes are Defined here: http://airflow.incubator.apache.org/cli.html
"""

# todo: dynamically decide which api objects to display based off which version of airflow is installed - http://stackoverflow.com/questions/1714027/version-number-comparison

# Location of the REST Endpoint
# Note: Changing this will only effect where the messages are posted to on the web interface and will not change where the endpoint actually resides
rest_api_endpoint = "/admin/rest_api/api"

# Getting Versions and Global variables
hostname = socket.gethostname()
airflow_version = airflow.__version__
rest_api_plugin_version = __version__

# Getting configurations from airflow.cfg file
airflow_webserver_base_url = configuration.get('webserver', 'BASE_URL')
airflow_base_log_folder = configuration.get('core', 'BASE_LOG_FOLDER')
airflow_dags_folder = configuration.get('core', 'DAGS_FOLDER')
log_loading = configuration.getboolean("rest_api_plugin", "LOG_LOADING") if configuration.has_option("rest_api_plugin", "LOG_LOADING") else False
filter_loading_messages_in_cli_response = configuration.getboolean("rest_api_plugin", "FILTER_LOADING_MESSAGES_IN_CLI_RESPONSE") if configuration.has_option("rest_api_plugin", "FILTER_LOADING_MESSAGES_IN_CLI_RESPONSE") else True
airflow_rest_api_plugin_http_token_header_name = configuration.get("rest_api_plugin", "REST_API_PLUGIN_HTTP_TOKEN_HEADER_NAME") if configuration.has_option("rest_api_plugin", "REST_API_PLUGIN_HTTP_TOKEN_HEADER_NAME") else "rest_api_plugin_http_token"
airflow_expected_http_token = configuration.get("rest_api_plugin", "REST_API_PLUGIN_EXPECTED_HTTP_TOKEN") if configuration.has_option("rest_api_plugin", "REST_API_PLUGIN_EXPECTED_HTTP_TOKEN") else None

# Using UTF-8 Encoding so that response messages don't have any characters in them that can't be handled
os.environ['PYTHONIOENCODING'] = 'utf-8'

if log_loading:
    logging.info("Initializing Airflow REST API Plugin with configs:")
    logging.info("\trest_api_endpoint: " + str(rest_api_endpoint))
    logging.info("\thostname: " + str(hostname))
    logging.info("\tairflow_version: " + str(airflow_version))
    logging.info("\trest_api_plugin_version: " + str(rest_api_plugin_version))
    logging.info("\tairflow_webserver_base_url: " + str(airflow_webserver_base_url))
    logging.info("\tairflow_base_log_folder: " + str(airflow_base_log_folder))
    logging.info("\tairflow_dags_folder: " + str(airflow_dags_folder))
    logging.info("\tairflow_rest_api_plugin_http_token_header_name: " + str(airflow_rest_api_plugin_http_token_header_name))
    logging.info("\tairflow_expected_http_token: OMITTED_FOR_SECURITY")
    logging.info("\tfilter_loading_messages_in_cli_response: " + str(filter_loading_messages_in_cli_response))

"""
Metadata that defines a single API:
{
    "name": "{string}",                     # Name of the API (cli command to be executed)
    "description": "{string}",              # Description of the API
    "airflow_version": "{string}",          # Version the API was available in to allow people to better determine if the API is available. (to be displayed on the Admin page)
    "http_method": "{string}",              # HTTP method to use when calling the function. (Default: GET) (Optional)
    "background_mode": {boolean},           # Whether to run the process in the background if its a CLI API (Optional)
    "arguments": [                          # List of arguments that can be provided to the API
        {
            "name": "{string}",             # Name of the argument
            "description": "{string}",      # Description of the argument
            "form_input_type": "{string}",  # Type of input to use on the Admin page for the argument
            "required": {boolean},          # Whether the argument is required upon submission
            "cli_end_position": {int}       # In the case with a CLI command that the arguments value should be appended on to the end (for example: airflow trigger_dag some_dag_id), this is the position that the argument should be provided in the CLI command. (Optional)
        }
    ],
    "fixed_arguments": [                    # List of arguments that will always be used by the API endpoint and can't be changed
        {
            "name": "{string}",             # Name of the argument
            "description": "{string}",      # Description of the argument
            "fixed_value": "{string}"       # Fixed value that will always be used
        }
    ],
    "post_arguments": [                     # List of arguments that can be provided in the POST body to the API
        {
            "name": "{string}",             # Name of the argument
            "description": "{string}",      # Description of the argument
            "form_input_type": "{string}",  # Type of input to use on the Admin page for the argument
            "required": {boolean},          # Whether the argument is required upon submission
        }
    ]
},
"""

# Metadata about the APIs and how to call them. Representing them like this allows us to dynamically generate the APIs
# in the admin page and dynamically execute them. This also allows us to easily add new ones.
# API Object definition is described in the comment block above.
apis_metadata = [
    {
        "name": "version",
        "description": "Displays the version of Airflow you're using",
        "airflow_version": "1.0.0 or greater",
        "http_method": "GET",
        "arguments": []
    },
    {
        "name": "rest_api_plugin_version",
        "description": "Displays the version of this REST API Plugin you're using",
        "airflow_version": "None - Custom API",
        "http_method": "GET",
        "arguments": []
    },
    {
        "name": "render",
        "description": "Render a task instance's template(s)",
        "airflow_version": "1.7.0 or greater",
        "http_method": "GET",
        "arguments": [
            {"name": "dag_id", "description": "The id of the dag", "form_input_type": "text", "required": True, "cli_end_position": 1},
            {"name": "task_id", "description": "The id of the task", "form_input_type": "text", "required": True, "cli_end_position": 2},
            {"name": "execution_date", "description": "The execution date of the DAG (Example: 2017-01-02T03:04:05)", "form_input_type": "text", "required": True, "cli_end_position": 3},
            {"name": "subdir", "description": "File location or directory from which to look for the dag", "form_input_type": "text", "required": False}
        ]
    },
    {
        "name": "variables",
        "description": "CRUD operations on variables",
        "airflow_version": "1.7.1 or greater",
        "http_method": "GET",
        "arguments": [
            {"name": "set", "description": "Set a variable. Expected input in the form: KEY VALUE.", "form_input_type": "text", "required": False},
            {"name": "get", "description": "Get value of a variable", "form_input_type": "text", "required": False},
            {"name": "json", "description": "Deserialize JSON variable", "form_input_type": "checkbox", "required": False},
            {"name": "default", "description": "Default value returned if variable does not exist", "form_input_type": "text", "required": False},
            {"name": "import", "description": "Import variables from JSON file", "form_input_type": "text", "required": False},
            {"name": "export", "description": "Export variables to JSON file", "form_input_type": "text", "required": False},
            {"name": "delete", "description": "Delete a variable", "form_input_type": "text", "required": False}
        ]
    },
    {
        "name": "connections",
        "description": "List/Add/Delete connections",
        "airflow_version": "1.8.0 or greater",
        "http_method": "GET",
        "arguments": [
            {"name": "list", "description": "List all connections", "form_input_type": "checkbox", "required": False},
            {"name": "add", "description": "Add a connection", "form_input_type": "checkbox", "required": False},
            {"name": "delete", "description": "Delete a connection", "form_input_type": "checkbox", "required": False},
            {"name": "conn_id", "description": "Connection id, required to add/delete a connection", "form_input_type": "text", "required": False},
            {"name": "conn_uri", "description": "Connection URI, required to add a connection", "form_input_type": "text", "required": False},
            {"name": "conn_extra", "description": "Connection 'Extra' field, optional when adding a connection", "form_input_type": "text", "required": False}
        ]
    },
    {
        "name": "pause",
        "description": "Pauses a DAG",
        "airflow_version": "1.7.0 or greater",
        "http_method": "GET",
        "arguments": [
            {"name": "dag_id", "description": "The id of the dag", "form_input_type": "text", "required": True, "cli_end_position": 1},
            {"name": "subdir", "description": "File location or directory from which to look for the dag", "form_input_type": "text", "required": False}
        ]
    },
    {
        "name": "unpause",
        "description": "Unpauses a DAG",
        "airflow_version": "1.7.0 or greater",
        "http_method": "GET",
        "arguments": [
            {"name": "dag_id", "description": "The id of the dag", "form_input_type": "text", "required": True, "cli_end_position": 1},
            {"name": "subdir", "description": "File location or directory from which to look for the dag", "form_input_type": "text", "required": False}
        ]
    },
    {
        "name": "task_failed_deps",
        "description": "Returns the unmet dependencies for a task instance from the perspective of the scheduler. In other words, why a task instance doesn't get scheduled and then queued by the scheduler, and then run by an executor).",
        "airflow_version": "1.8.0 or greater",
        "http_method": "GET",
        "arguments": [
            {"name": "dag_id", "description": "The id of the dag", "form_input_type": "text", "required": True, "cli_end_position": 1},
            {"name": "task_id", "description": "The id of the task", "form_input_type": "text", "required": True, "cli_end_position": 2},
            {"name": "execution_date", "description": "The execution date of the DAG (Example: 2017-01-02T03:04:05)", "form_input_type": "text", "required": True, "cli_end_position": 3},
            {"name": "subdir", "description": "File location or directory from which to look for the dag", "form_input_type": "text", "required": False}
        ]
    },
    {   # todo: should print out the run id
        "name": "trigger_dag",
        "description": "Trigger a DAG run",
        "airflow_version": "1.6.0 or greater",
        "http_method": "GET",
        "arguments": [
            {"name": "dag_id", "description": "The id of the dag", "form_input_type": "text", "required": True, "cli_end_position": 1},
            {"name": "subdir", "description": "File location or directory from which to look for the dag", "form_input_type": "text", "required": False},
            {"name": "run_id", "description": "Helps to identify this run", "form_input_type": "text", "required": False},
            {"name": "conf", "description": "JSON string that gets pickled into the DagRun's conf attribute", "form_input_type": "text", "required": False},
            {"name": "exec_date", "description": "The execution date of the DAG", "form_input_type": "text", "required": False}
        ]
    },
    {
        "name": "test",
        "description": "Test a task instance. This will run a task without checking for dependencies or recording it's state in the database.",
        "airflow_version": "0.1 or greater",
        "http_method": "GET",
        "arguments": [
            {"name": "dag_id", "description": "The id of the dag", "form_input_type": "text", "required": True, "cli_end_position": 1},
            {"name": "task_id", "description": "The id of the task", "form_input_type": "text", "required": True, "cli_end_position": 2},
            {"name": "execution_date", "description": "The execution date of the DAG (Example: 2017-01-02T03:04:05)", "form_input_type": "text", "required": True, "cli_end_position": 3},
            {"name": "subdir", "description": "File location or directory from which to look for the dag", "form_input_type": "text", "required": False},
            {"name": "dry_run", "description": "Perform a dry run", "form_input_type": "checkbox", "required": False},
            {"name": "task_params", "description": "Sends a JSON params dict to the task", "form_input_type": "text", "required": False}
        ]
    },
    {
        "name": "dag_state",
        "description": "Get the status of a dag run",
        "airflow_version": "1.8.0 or greater",
        "http_method": "GET",
        "arguments": [
            {"name": "dag_id", "description": "The id of the dag", "form_input_type": "text", "required": True, "cli_end_position": 1},
            {"name": "execution_date", "description": "The execution date of the DAG (Example: 2017-01-02T03:04:05)", "form_input_type": "text", "required": True, "cli_end_position": 2},
            {"name": "subdir", "description": "File location or directory from which to look for the dag", "form_input_type": "text", "required": False}
        ]
    },
    {
        "name": "run",
        "description": "Run a single task instance",
        "airflow_version": "1.0.0 or greater",
        "http_method": "GET",
        "arguments": [
            {"name": "dag_id", "description": "The id of the dag", "form_input_type": "text", "required": True, "cli_end_position": 1},
            {"name": "task_id", "description": "The id of the task", "form_input_type": "text", "required": True, "cli_end_position": 2},
            {"name": "execution_date", "description": "The execution date of the DAG (Example: 2017-01-02T03:04:05)", "form_input_type": "text", "required": True, "cli_end_position": 3},
            {"name": "subdir", "description": "File location or directory from which to look for the dag", "form_input_type": "text", "required": False},
            {"name": "mark_success", "description": "Mark jobs as succeeded without running them", "form_input_type": "checkbox", "required": False},
            {"name": "force", "description": "Ignore previous task instance state, rerun regardless if task already succeede", "form_input_type": "checkbox", "required": False},
            {"name": "pool", "description": "Resource pool to use", "form_input_type": "text", "required": False},
            {"name": "cfg_path", "description": "Path to config file to use instead of airflow.cfg", "form_input_type": "text", "required": False},
            {"name": "local", "description": "Run the task using the LocalExecutor", "form_input_type": "checkbox", "required": False},
            {"name": "ignore_all_dependencies", "description": "Ignores all non-critical dependencies, including ignore_ti_state and ignore_task_depsstore_true", "form_input_type": "checkbox", "required": False},
            {"name": "ignore_dependencies", "description": "Ignore task-specific dependencies, e.g. upstream, depends_on_past, and retry delay dependencies", "form_input_type": "checkbox", "required": False},
            {"name": "ignore_depends_on_past", "description": "Ignore depends_on_past dependencies (but respect upstream dependencies)", "form_input_type": "checkbox", "required": False},
            {"name": "ship_dag", "description": "Pickles (serializes) the DAG and ships it to the worker", "form_input_type": "checkbox", "required": False},
            {"name": "pickle", "description": "Serialized pickle object of the entire dag (used internally)", "form_input_type": "text", "required": False},
        ]
    },
    {
        "name": "list_tasks",
        "description": "List the tasks within a DAG",
        "airflow_version": "0.1 or greater",
        "http_method": "GET",
        "arguments": [
            {"name": "dag_id", "description": "The id of the dag", "form_input_type": "text", "required": True, "cli_end_position": 1},
            {"name": "tree", "description": "Tree view", "form_input_type": "checkbox", "required": False},
            {"name": "subdir", "description": "File location or directory from which to look for the dag", "form_input_type": "text", "required": False}
        ]
    },
    {
        "name": "backfill",
        "description": "Run subsections of a DAG for a specified date range",
        "airflow_version": "0.1 or greater",
        "http_method": "GET",
        "arguments": [
            {"name": "dag_id", "description": "The id of the dag", "form_input_type": "text", "required": True, "cli_end_position": 1},
            {"name": "task_regex", "description": "The regex to filter specific task_ids to backfill (optional)", "form_input_type": "text", "required": False},
            {"name": "start_date", "description": "Override start_date YYYY-MM-DD. Either this or the end_date needs to be provided.", "form_input_type": "text", "required": False},
            {"name": "end_date", "description": "Override end_date YYYY-MM-DD. Either this or the start_date needs to be provided.", "form_input_type": "text", "required": False},
            {"name": "mark_success", "description": "Mark jobs as succeeded without running them", "form_input_type": "checkbox", "required": False},
            {"name": "local", "description": "Run the task using the LocalExecutor", "form_input_type": "checkbox", "required": False},
            {"name": "donot_pickle", "description": "Do not attempt to pickle the DAG object to send over to the workers, just tell the workers to run their version of the code.", "form_input_type": "checkbox", "required": False},
            {"name": "include_adhoc", "description": "Include dags with the adhoc argument.", "form_input_type": "checkbox", "required": False},
            {"name": "ignore_dependencies", "description": "Ignore task-specific dependencies, e.g. upstream, depends_on_past, and retry delay dependencies", "form_input_type": "checkbox", "required": False},
            {"name": "ignore_first_depends_on_past", "description": "Ignores depends_on_past dependencies for the first set of tasks only (subsequent executions in the backfill DO respect depends_on_past).", "form_input_type": "checkbox", "required": False},
            {"name": "subdir", "description": "File location or directory from which to look for the dag", "form_input_type": "text", "required": False},
            {"name": "pool", "description": "Resource pool to use", "form_input_type": "text", "required": False},
            {"name": "dry_run", "description": "Perform a dry run", "form_input_type": "checkbox", "required": False}
        ]
    },
    {
        "name": "list_dags",
        "description": "List all the DAGs",
        "airflow_version": "0.1 or greater",
        "http_method": "GET",
        "arguments": [
            {"name": "subdir", "description": "File location or directory from which to look for the dag", "form_input_type": "text", "required": False},
            {"name": "report", "description": "Show DagBag loading report", "form_input_type": "checkbox", "required": False}
        ]
    },
    {
        "name": "kerberos",
        "description": "Start a kerberos ticket renewer",
        "airflow_version": "1.6.0 or greater",
        "http_method": "GET",
        "background_mode": True,
        "arguments": [
            {"name": "principal", "description": "kerberos principal", "form_input_type": "text", "required": True, "cli_end_position": 1},
            {"name": "keytab", "description": "keytab", "form_input_type": "text", "required": False},
            {"name": "pid", "description": "PID file location", "form_input_type": "text", "required": False},
            {"name": "daemon", "description": "Daemonize instead of running in the foreground", "form_input_type": "checkbox", "required": False},
            {"name": "stdout", "description": "Redirect stdout to this file", "form_input_type": "text", "required": False},
            {"name": "stderr", "description": "Redirect stderr to this file", "form_input_type": "text", "required": False},
            {"name": "log-file", "description": "Location of the log file", "form_input_type": "text", "required": False}
        ]
    },
    {
        "name": "worker",
        "description": "Start a Celery worker node",
        "airflow_version": "0.1 or greater",
        "http_method": "GET",
        "background_mode": True,
        "arguments": [
            {"name": "do_pickle", "description": "Attempt to pickle the DAG object to send over to the workers, instead of letting workers run their version of the code.", "form_input_type": "checkbox", "required": False},
            {"name": "queues", "description": "Comma delimited list of queues to serve", "form_input_type": "text", "required": False},
            {"name": "concurrency", "description": "The number of worker processes", "form_input_type": "text", "required": False},
            {"name": "pid", "description": "PID file location", "form_input_type": "checkbox", "required": False},
            {"name": "daemon", "description": "Daemonize instead of running in the foreground", "form_input_type": "checkbox", "required": False},
            {"name": "stdout", "description": "Redirect stdout to this file", "form_input_type": "text", "required": False},
            {"name": "stderr", "description": "Redirect stderr to this file", "form_input_type": "text", "required": False},
            {"name": "log-file", "description": "Location of the log file", "form_input_type": "text", "required": False}
        ]
    },
    {
        "name": "flower",
        "description": "Start a Celery worker node",
        "airflow_version": "1.0.0 or greater",
        "http_method": "GET",
        "background_mode": True,
        "arguments": [
            {"name": "hostname", "description": "Set the hostname on which to run the server", "form_input_type": "text", "required": False},
            {"name": "port", "description": "The port on which to run the server", "form_input_type": "text", "required": False},
            {"name": "flower_conf", "description": "Configuration file for flower", "form_input_type": "text", "required": False},
            {"name": "broker_api", "description": "Broker api", "form_input_type": "text", "required": False},
            {"name": "pid", "description": "PID file location", "form_input_type": "text", "required": False},
            {"name": "daemon", "description": "Daemonize instead of running in the foreground", "form_input_type": "checkbox", "required": False},
            {"name": "stdout", "description": "Redirect stdout to this file", "form_input_type": "text", "required": False},
            {"name": "stderr", "description": "Redirect stderr to this file", "form_input_type": "text", "required": False},
            {"name": "log-file", "description": "Location of the log file", "form_input_type": "text", "required": False},
            ]
    },
    {
        "name": "scheduler",
        "description": "Start a scheduler instance",
        "airflow_version": "1.0.0 or greater",
        "http_method": "GET",
        "background_mode": True,
        "arguments": [
            {"name": "dag_id", "description": "The id of the dag", "form_input_type": "text", "required": False},
            {"name": "subdir", "description": "File location or directory from which to look for the dag", "form_input_type": "text", "required": False},
            {"name": "run-duration", "description": "Set number of seconds to execute before exiting", "form_input_type": "text", "required": False},
            {"name": "num_runs", "description": "Set the number of runs to execute before exiting", "form_input_type": "text", "required": False},
            {"name": "do_pickle", "description": "Attempt to pickle the DAG object to send over to the workers, instead of letting workers run their version of the code.", "form_input_type": "text", "required": False},
            {"name": "pid", "description": "PID file location", "form_input_type": "checkbox", "required": False},
            {"name": "daemon", "description": "Daemonize instead of running in the foreground", "form_input_type": "checkbox", "required": False},
            {"name": "stdout", "description": "Redirect stdout to this file", "form_input_type": "text", "required": False},
            {"name": "stderr", "description": "Redirect stderr to this file", "form_input_type": "text", "required": False},
            {"name": "log-file", "description": "Location of the log file", "form_input_type": "text", "required": False}
        ]
    },
    {
        "name": "task_state",
        "description": "Get the status of a task instance",
        "airflow_version": "1.0.0 or greater",
        "http_method": "GET",
        "arguments": [
            {"name": "dag_id", "description": "The id of the dag", "form_input_type": "text", "required": True, "cli_end_position": 1},
            {"name": "task_id", "description": "The id of the task", "form_input_type": "text", "required": True, "cli_end_position": 2},
            {"name": "execution_date", "description": "The execution date of the DAG (Example: 2017-01-02T03:04:05)", "form_input_type": "text", "required": True, "cli_end_position": 3},
            {"name": "subdir", "description": "File location or directory from which to look for the dag", "form_input_type": "text", "required": False}
        ]
    },
    {
        "name": "pool",
        "description": "CRUD operations on pools",
        "airflow_version": "1.8.0 or greater",
        "http_method": "GET",
        "arguments": [
            {"name": "set", "description": "Set pool slot count and description, respectively. Expected input in the form: NAME SLOT_COUNT POOL_DESCRIPTION.", "form_input_type": "text", "required": False},
            {"name": "get", "description": "Get pool info", "form_input_type": "text", "required": False},
            {"name": "delete", "description": "Delete a pool", "form_input_type": "text", "required": False}
        ]
    },
    {
        "name": "serve_logs",
        "description": "Serve logs generate by worker",
        "airflow_version": "0.1 or greater",
        "http_method": "GET",
        "background_mode": True,
        "arguments": []
    },
    {
        "name": "clear",
        "description": "Clear a set of task instance, as if they never ran",
        "airflow_version": "0.1 or greater",
        "http_method": "GET",
        "arguments": [
            {"name": "dag_id", "description": "The id of the dag", "form_input_type": "text", "required": True, "cli_end_position": 1},
            {"name": "task_regex", "description": "The regex to filter specific task_ids to backfill (optional)", "form_input_type": "text", "required": False},
            {"name": "start_date", "description": "Override start_date YYYY-MM-DD", "form_input_type": "text", "required": False},
            {"name": "end_date", "description": "Override end_date YYYY-MM-DD", "form_input_type": "text", "required": False},
            {"name": "subdir", "description": "File location or directory from which to look for the dag", "form_input_type": "text", "required": False},
            {"name": "upstream", "description": "Include upstream tasks", "form_input_type": "checkbox", "required": False},
            {"name": "downstream", "description": "Include downstream tasks", "form_input_type": "checkbox", "required": False},
            {"name": "only_failed", "description": "Only failed jobs", "form_input_type": "checkbox", "required": False},
            {"name": "only_running", "description": "Only running jobs", "form_input_type": "checkbox", "required": False},
            {"name": "exclude_subdags", "description": "Exclude subdags", "form_input_type": "checkbox", "required": False}
        ],
        "fixed_arguments": [
            {"name": "no_confirm", "description": "Do not request confirmation", "fixed_value": ""}
        ],
    },
    {
        "name": "deploy_dag",
        "description": "Deploy a new DAG File to the DAGs directory",
        "airflow_version": "None - Custom API",
        "http_method": "POST",
        "post_body_description": "dag_file - POST Body Element - REQUIRED",
        "form_enctype": "multipart/form-data",
        "arguments": [],
        "post_arguments": [
            {"name": "dag_file", "description": "Python file to upload and deploy", "form_input_type": "file", "required": True},
            {"name": "force", "description": "Whether to forcefully upload the file if the file already exists or not", "form_input_type": "checkbox", "required": False},
            {"name": "pause", "description": "The DAG will be forced to be paused when created and override the 'dags_are_paused_at_creation' config.", "form_input_type": "checkbox", "required": False},
            {"name": "unpause", "description": "The DAG will be forced to be unpaused when created and override the 'dags_are_paused_at_creation' config.", "form_input_type": "checkbox", "required": False}
        ]
    },
    {
        "name": "refresh_dag",
        "description": "Refresh a DAG in the Web Server",
        "airflow_version": "None - Custom API",
        "http_method": "GET",
        "arguments": [
            {"name": "dag_id", "description": "The id of the dag", "form_input_type": "text", "required": True}
        ]
    }
]


# Function used to secure the REST ENDPOINT
def http_token_secure(func):
    def secure_check(arg):
        logging.info("Rest_API_Plugin.http_token_secure() called")
        # Check if the airflow_expected_http_token variable is not none from configurations. This means authentication is enabled.
        if airflow_expected_http_token:
            logging.info("Performing Token Authentication")
            if request.headers.get(airflow_rest_api_plugin_http_token_header_name, None) != airflow_expected_http_token:
                warning_message = "Token Authentication Failed"
                logging.warn(warning_message)
                base_response = REST_API_Response_Util.get_base_response(include_arguments=False)
                return REST_API_Response_Util.get_403_error_response(base_response=base_response, output=warning_message)
        return func(arg)

    return secure_check


# Utility for creating the REST Responses
class REST_API_Response_Util():

    # Gets the Base Response object with all required response fields included. To be used at the beginning of the REST Call.
    @staticmethod
    def get_base_response(status="OK", http_response_code=200, call_time=datetime.now(), include_arguments=True):
        base_response = {"status": status, "http_response_code": http_response_code, "call_time": call_time}
        if include_arguments:
            base_response["arguments"] = request.args
            base_response["post_arguments"] = request.form
        return base_response

    # Finalize the Base Response with additional data
    @staticmethod
    def _get_final_response(base_response, output=None, airflow_cmd=None, http_response_code=None, warning=None):
        final_response = base_response
        final_response["response_time"] = datetime.now()
        if output:
            final_response["output"] = output
        if airflow_cmd:
            final_response["airflow_cmd"] = airflow_cmd
        if http_response_code:
            final_response["http_response_code"] = http_response_code
        if warning:
            final_response["warning"] = warning
        return jsonify(final_response)

    # Set the Base Response as a 200 HTTP Response object
    @staticmethod
    def get_200_response(base_response, output=None, airflow_cmd=None, warning=None):
        logging.info("Returning a 200 Response Code with response '" + str(output) + "'")
        return REST_API_Response_Util._get_final_response(base_response=base_response, output=output, airflow_cmd=airflow_cmd, warning=warning)

    # Set the Base Response and an Error
    @staticmethod
    def _get_error_response(base_response, error_code, output=None):
        base_response["status"] = "ERROR"
        return REST_API_Response_Util._get_final_response(base_response=base_response, output=output, http_response_code=error_code), error_code

    # Set the Base Response as a 400 HTTP Response object
    @staticmethod
    def get_400_error_response(base_response, output=None):
        logging.warning("Returning a 400 Response Code with response '" + str(output) + "'")
        return REST_API_Response_Util._get_error_response(base_response, 400, output)

    # Set the Base Response as a 403 HTTP Response object
    @staticmethod
    def get_403_error_response(base_response, output=None):
        logging.warning("Returning a 403 Response Code with response '" + str(output) + "'")
        return REST_API_Response_Util._get_error_response(base_response, 403, output)

    # Set the Base Response as a 500 HTTP Response object
    @staticmethod
    def get_500_error_response(base_response, output=None):
        logging.warning("Returning a 500 Response Code with response '" + str(output) + "'")
        return REST_API_Response_Util._get_error_response(base_response, 500, output)


# REST_API View which extends the flask_admin BaseView
class REST_API(BaseView):

    # Checks a string object to see if it is none or empty so we can determine if an argument (passed to the rest api) is provided
    @staticmethod
    def is_arg_not_provided(arg):
        return arg is None or arg == ""

    # Get the DagBag which has a list of all the current Dags
    @staticmethod
    def get_dagbag():
        return DagBag()

    # '/' Endpoint where the Admin page is which allows you to view the APIs available and trigger them
    @expose('/')
    def index(self):
        logging.info("REST_API.index() called")

        # get the information that we want to display on the page regarding the dags that are available
        dagbag = self.get_dagbag()
        dags = []
        for dag_id in dagbag.dags:
            orm_dag = DagModel.get_current(dag_id)
            dags.append({
                "dag_id": dag_id,
                "is_active": (not orm_dag.is_paused) if orm_dag is not None else False
            })

        return self.render("rest_api_plugin/index.html",
                           dags=dags,
                           airflow_webserver_base_url=airflow_webserver_base_url,
                           rest_api_endpoint=rest_api_endpoint,
                           apis_metadata=apis_metadata,
                           airflow_version=airflow_version,
                           rest_api_plugin_version=rest_api_plugin_version
                           )

    # '/api' REST Endpoint where API requests should all come in
    @csrf.exempt  # Exempt the CSRF token
    @expose('/api', methods=["GET", "POST"])
    @http_token_secure  # On each request,
    def api(self):
        base_response = REST_API_Response_Util.get_base_response()

        # Get the api that you want to execute
        api = request.args.get('api')
        if api is not None:
            api = api.strip().lower()
        logging.info("REST_API.api() called (api: " + str(api) + ")")

        # Validate that the API is provided
        if self.is_arg_not_provided(api):
            logging.warning("api argument not provided")
            return REST_API_Response_Util.get_400_error_response(base_response, "API should be provided")

        # Get the api_metadata from the api object list that correcsponds to the api we want to run to get the metadata.
        api_metadata = None
        for test_api_metadata in apis_metadata:
            if test_api_metadata["name"] == api:
                api_metadata = test_api_metadata
        if api_metadata is None:
            logging.info("api '" + str(api) + "' was not found in the apis list in the REST API Plugin")
            return REST_API_Response_Util.get_400_error_response(base_response, "API '" + str(api) + "' was not found")

        # check if all the required arguments are provided
        missing_required_arguments = []
        dag_id = None
        for argument in api_metadata["arguments"]:
            argument_name = argument["name"]
            argument_value = request.args.get(argument_name)
            if argument["required"]:
                if self.is_arg_not_provided(argument_value):
                    missing_required_arguments.append(argument_name)
            if argument_name == "dag_id" and argument_value is not None:
                dag_id = argument_value.strip()
        if len(missing_required_arguments) > 0:
            logging.warning("Missing required arguments: " + str(missing_required_arguments))
            return REST_API_Response_Util.get_400_error_response(base_response, "The argument(s) " + str(missing_required_arguments) + " are required")

        # Check to make sure that the DAG you're referring to, already exists.
        dag_bag = self.get_dagbag()
        if dag_id is not None and dag_id not in dag_bag.dags:
            logging.info("DAG_ID '" + str(dag_id) + "' was not found in the DagBag list '" + str(dag_bag.dags) + "'")
            return REST_API_Response_Util.get_400_error_response(base_response, "The DAG ID '" + str(dag_id) + "' does not exist")

        # Deciding which function to use based off the API object that was requested. Some functions are custom and need to be manually routed to.
        if api == "version":
            final_response = self.version(base_response)
        elif api == "rest_api_plugin_version":
            final_response = self.rest_api_plugin_version(base_response)
        elif api == "deploy_dag":
            final_response = self.deploy_dag(base_response)
        elif api == "refresh_dag":
            final_response = self.refresh_dag(base_response)
        else:
            final_response = self.execute_cli(base_response, api_metadata)

        return final_response

    # General execution of a CLI command
    # A command will be assembled and then passed to the OS as a commandline function and the results will be returned
    def execute_cli(self, base_response, api_metadata):
        logging.info("Executing cli function")

        # getting the largest cli_end_position in the api_metadata object so that the cli function can be assembled
        largest_end_argument_value = 0
        for argument in api_metadata.get("arguments", []):
            if argument.get("cli_end_position") is not None and argument["cli_end_position"] > largest_end_argument_value:
                largest_end_argument_value = argument["cli_end_position"]

        # starting to create the airflow_cmd function
        airflow_cmd_split = ["airflow", api_metadata["name"]]

        # appending arguments to the airflow_cmd_split array and setting arguments aside in the end_arguments array to be appended onto the end of airflow_cmd_split
        end_arguments = [0] * largest_end_argument_value
        for argument in api_metadata["arguments"]:
            argument_name = argument["name"]
            argument_value = request.args.get(argument_name)
            logging.info("argument_name: " + str(argument_name) + ", argument_value: " + str(argument_value))
            if argument_value is not None:
                # if the argument should be appended onto the end, find the position and add it to the end_arguments array
                if "cli_end_position" in argument:
                    logging.info("argument['cli_end_position']: " + str(argument['cli_end_position']))
                    end_arguments[argument["cli_end_position"]-1] = argument_value
                else:
                    airflow_cmd_split.extend(["--" + argument_name])
                    if argument["form_input_type"] is not "checkbox":
                        # Relacing airflow_cmd_split.extend(argument_value.split(" ") with command below to fix issue where configuration 
                        # values contain space with them.
                        airflow_cmd_split.append(argument_value)
            else:
                logging.warning("argument_value is null")

        # appending fixed arguments that should always be provided to the APIs
        for fixed_argument in api_metadata.get("fixed_arguments", []):
            fixed_argument_name = fixed_argument["name"]
            fixed_argument_value = fixed_argument.get("fixed_value")
            logging.info("fixed_argument_name: " + str(fixed_argument_name) + ", fixed_argument_value: " + str(fixed_argument_value))
            if fixed_argument_value is not None:
                airflow_cmd_split.extend(["--" + fixed_argument_name])
                if fixed_argument_value:
                    airflow_cmd_split.extend(fixed_argument_value.split(" "))

        # appending the end_arguments to the very end
        airflow_cmd_split.extend(end_arguments)

        run_api_in_background_mode = "background_mode" in api_metadata and api_metadata["background_mode"]

        # handling the case where the process should be ran in the background
        if run_api_in_background_mode:
            # if a log file is provided, then that should be used to dump the output of the call
            if request.args.get("log-file") is None:
                airflow_cmd_split.append(">> " + str(airflow_base_log_folder) + "/" + api_metadata["name"] + ".log")
            # appending a '&' character to run the process in the background
            airflow_cmd_split.append("&")

        # joining all the individual arguments and components into a single string
        airflow_cmd = " ".join(airflow_cmd_split)

        logging.info("airflow_cmd array: " + str(airflow_cmd_split))
        logging.info("airflow_cmd: " + str(airflow_cmd))

        # execute the airflow command a certain way if its meant to be ran in the background
        if run_api_in_background_mode:
            output = self.execute_cli_command_background_mode(airflow_cmd)
        else:
            output = self.execute_cli_command(airflow_cmd_split)

        # if desired, filter out the loading messages to reduce the noise in the output
        if filter_loading_messages_in_cli_response:
            logging.info("Filtering Loading Messages from the CLI Response")
            output = self.filter_loading_messages(output)

        return REST_API_Response_Util.get_200_response(base_response=base_response, output=output, airflow_cmd=airflow_cmd)

    # Custom function for the version API
    def version(self, base_response):
        logging.info("Executing custom 'version' function")
        return REST_API_Response_Util.get_200_response(base_response, airflow_version)

    # Custom function for the rest_api_plugin_version API
    def rest_api_plugin_version(self, base_response):
        logging.info("Executing custom 'rest_api_plugin_version' function")
        return REST_API_Response_Util.get_200_response(base_response, rest_api_plugin_version)

    # Custom Function for the deploy_dag API
    def deploy_dag(self, base_response):
        logging.info("Executing custom 'deploy_dag' function")

        if 'dag_file' not in request.files or request.files['dag_file'].filename == '':  # check if the post request has the file part
            logging.warning("The dag_file argument wasn't provided")
            return REST_API_Response_Util.get_400_error_response(base_response, "dag_file should be provided")
        dag_file = request.files['dag_file']

        force = True if request.form.get('force') is not None else False
        logging.info("deploy_dag force upload: " + str(force))

        pause = True if request.form.get('pause') is not None else False
        logging.info("deploy_dag in pause state: " + str(pause))

        unpause = True if request.form.get('unpause') is not None else False
        logging.info("deploy_dag in unpause state: " + str(unpause))

        # make sure that the dag_file is a python script
        if dag_file and dag_file.filename.endswith(".py"):
            save_file_path = os.path.join(airflow_dags_folder, dag_file.filename)

            # Check if the file already exists.
            if os.path.isfile(save_file_path) and not force:
                logging.warning("File to upload already exists")
                return REST_API_Response_Util.get_400_error_response(base_response, "The file '" + save_file_path + "' already exists on host '" + hostname + "'.")

            logging.info("Saving file to '" + save_file_path + "'")
            dag_file.save(save_file_path)

        else:
            logging.warning("deploy_dag file is not a python file. It does not end with a .py.")
            return REST_API_Response_Util.get_400_error_response(base_response, "dag_file is not a *.py file")

        warning = None
        # if both the pause and unpause options are provided then skip the pausing and unpausing phase
        if not (pause and unpause):
            if pause or unpause:
                try:
                    # import the DAG file that was uploaded so that we can get the DAG_ID to execute the command to pause or unpause it
                    import imp
                    dag_file = imp.load_source('module.name', save_file_path)
                    dag_id = dag_file.dag.dag_id

                    # run the pause or unpause cli command
                    airflow_cmd_split = []
                    if pause:
                        airflow_cmd_split = ["airflow", "pause", dag_id]
                    if unpause:
                        airflow_cmd_split = ["airflow", "unpause", dag_id]
                    cli_output = self.execute_cli_command(airflow_cmd_split)
                except Exception as e:
                    warning = "Failed to set the state (pause, unpause) of the DAG: " + str(e)
                    logging.warning(warning)
        else:
            warning = "Both options pause and unpause were given. Skipping setting the state (pause, unpause) of the DAG."
            logging.warning(warning)

        return REST_API_Response_Util.get_200_response(base_response=base_response, output="DAG File [{}] has been uploaded".format(dag_file), warning=warning)

    # Custom Function for the refresh_dag API
    # This will call the direct function corresponding to the web endpoint '/admin/airflow/refresh' that already exists in Airflow
    def refresh_dag(self, base_response):
        logging.info("Executing custom 'refresh_dag' function")
        dag_id = request.args.get('dag_id')
        logging.info("dag_id to refresh: '" + str(dag_id) + "'")
        if self.is_arg_not_provided(dag_id):
            return REST_API_Response_Util.get_400_error_response(base_response, "dag_id should be provided")
        elif " " in dag_id:
            return REST_API_Response_Util.get_400_error_response(base_response, "dag_id contains spaces and is therefore an illegal argument")

        try:
            from airflow.www.views import Airflow
            # NOTE: The request argument 'dag_id' is required for the refresh() function to get the dag_id
            refresh_result = Airflow().refresh()
            logging.info("Refresh Result: " + str(refresh_result))
        except Exception as e:
            error_message = "An error occurred while trying to Refresh the DAG '" + str(dag_id) + "': " + str(e)
            logging.error(error_message)
            return REST_API_Response_Util.get_500_error_response(base_response, error_message)

        return REST_API_Response_Util.get_200_response(base_response=base_response, output="DAG [{}] is now fresh as a daisy".format(dag_id))

    # Executes the airflow command passed into it in the background so the function isn't tied to the webserver process
    @staticmethod
    def execute_cli_command_background_mode(airflow_cmd):
        logging.info("Executing CLI Command in the Background")
        exit_code = os.system(airflow_cmd)
        output = REST_API.get_empty_process_output()
        output["stdout"] = "exit_code: " + str(exit_code)
        return output

    # General execution of the airflow command passed to it and returns the response
    @staticmethod
    def execute_cli_command(airflow_cmd_split):
        logging.info("Executing CLI Command")
        process = subprocess.Popen(airflow_cmd_split, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.wait()
        return REST_API.collect_process_output(process)

    # gets and empty object that has all the fields a CLI function would have in it.
    @staticmethod
    def get_empty_process_output():
        return {
            "stderr": "",
            "stdin": "",
            "stdout": ""
        }

    # Get the output of the CLI process and package it in a dict
    @staticmethod
    def collect_process_output(process):
        output = REST_API.get_empty_process_output()
        if process.stderr is not None:
            output["stderr"] = ""
            for line in process.stderr.readlines():
                output["stderr"] += str(line)
        if process.stdin is not None:
            output["stdin"] = ""
            for line in process.stdin.readlines():
                output["stdin"] += str(line)
        if process.stdout is not None:
            output["stdout"] = ""
            for line in process.stdout.readlines():
                output["stdout"] += str(line)
        logging.info("RestAPI Output: " + str(output))
        return output

    # Filtering out logging statements from the standard output
    # Content like:
    #
    # [2017-04-19 10:04:34,927] {__init__.py:36} INFO - Using executor CeleryExecutor
    # [2017-04-19 10:04:35,926] {models.py:154} INFO - Filling up the DagBag from /Users/...
    @staticmethod
    def filter_loading_messages(output):
        stdout = output["stdout"]
        new_stdout_array = stdout.split("\n")
        content_to_remove_greatest_index = 0
        for index, content in enumerate(new_stdout_array):
            if content.startswith("["):
                content_to_remove_greatest_index = index
        content_to_remove_greatest_index += 1
        if len(new_stdout_array) > content_to_remove_greatest_index:
            new_stdout_array = new_stdout_array[content_to_remove_greatest_index:]
            output["stdout"] = "\n".join(new_stdout_array)
        return output

# Creating View to be used by Plugin
rest_api_view = REST_API(category="Admin", name="REST API Plugin")

# Creating Blueprint
rest_api_bp = Blueprint(
    "rest_api_bp",
    __name__,
    template_folder='templates',
    static_folder='static',
    static_url_path='/static/'
)


# Creating the REST_API_Plugin which extends the AirflowPlugin so its imported into Airflow
class REST_API_Plugin(AirflowPlugin):
    name = "rest_api"
    operators = []
    flask_blueprints = [rest_api_bp]
    hooks = []
    executors = []
    admin_views = [rest_api_view]
    menu_links = []
