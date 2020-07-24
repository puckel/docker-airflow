#!/usr/bin/env bash

# Install custom python package if requirements.txt is present
if [[ -e "/requirements.txt" ]]; then
    $(command -v pip) install --user -r /requirements.txt
fi

case "$1" in
  webserver|worker|flower)
    # Give the scheduler time to run upgradedb.
    sleep 20
    exec /entrypoint "$@"
    ;;
  scheduler)
    # Give postgres time to come up.
    sleep 10
    airflow upgradedb
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
