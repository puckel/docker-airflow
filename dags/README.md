# DAGs
This folder contains the code for all DAGs that are listed in the UI. Below is a list of additional configuration you can pass to the individual dags when triggering a run. If the DAG is not listed below, then there is no additional configuration to pass in.

## experimental_backfill
You may pass a JSON object with the following keys and values:

1. do_complete_backfill (boolean, default: false) -- By default this dag will only backfill the last four weeks of data. If this boolean is set to true, then the backfill will be for all time
2. limit_experiment_ids (list of strings, default: None)-- By default this DAG will backfill the results for all experiments. However, you can limit the experiments to backfill by passing in a list of experiment ids as strings. Experiment ids can be found on the experimental_control_panel. (example: ["1b991ced-c0a3-4d18-9020-b8fea6115a90"])