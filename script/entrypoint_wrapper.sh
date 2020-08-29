#!/usr/bin/env bash

# Install custom python package if requirements.txt is present
if [[ -e "/requirements.txt" ]]; then
    $(command -v pip) install --user -r /requirements.txt
fi

case "$1" in
  webserver|worker|flower)
    # Give the scheduler time to run upgradedb.
    sleep 10
    exec /entrypoint "$@"
    ;;
  scheduler)
    echo "Attempting upgradedb command.."
    # In upgradedb default connections are not populated. Use "airflow initdb" instead for default connections.
    airflow upgradedb
    if [[ "$AIRFLOW__CORE__EXECUTOR" = "LocalExecutor" ]] || [[ "$AIRFLOW__CORE__EXECUTOR" = "SequentialExecutor" ]];
    then
      # Running webserver in scheduler instead of reverse to maintain consistency in Makefile.
      # With the "Local" and "Sequential" executors it should all run in one container.
      airflow webserver &
    fi
    exec /entrypoint "$@"
    ;;
  bash|python)
    exec /entrypoint "$@"
    ;;
  *)
    # The command is something like bash, not an airflow subcommand. Just run it in the right environment.
    exec /entrypoint "$@"
    ;;
esac
