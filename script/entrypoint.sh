#!/usr/bin/env bash

CMD="airflow"
TRY_LOOP="20"

: "${REDIS_HOST:="redis"}"
: "${REDIS_PORT:="6379"}"
: "${REDIS_PASSWORD:=""}"

: "${POSTGRES_HOST:="postgres"}"
: "${POSTGRES_PORT:="5432"}"
: "${POSTGRES_USER:="airflow"}"
: "${POSTGRES_PASSWORD:="airflow"}"
: "${POSTGRES_DB:="airflow"}"

# Defaults and back-compat
: "${AIRFLOW__CORE__FERNET_KEY:=${FERNET_KEY:=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")}}"
: "${AIRFLOW__CORE__EXECUTOR:=${EXECUTOR:-Sequential}Executor}"

export \
  AIRFLOW__CELERY__BROKER_URL \
  AIRFLOW__CELERY__CELERY_RESULT_BACKEND \
  AIRFLOW__CORE__EXECUTOR \
  AIRFLOW__CORE__FERNET_KEY \
  AIRFLOW__CORE__LOAD_EXAMPLES \
  AIRFLOW__CORE__SQL_ALCHEMY_CONN \


# Load DAGs exemples (default: Yes)
if [[ -z "$AIRFLOW__CORE__LOAD_EXAMPLES" && "${LOAD_EX:=n}" == n ]]
then
  AIRFLOW__CORE__LOAD_EXAMPLES=False
fi

# Install custome python package if requirements.txt is present
if [ -e "/requirements.txt" ]; then
    $(which pip) install --user -r /requirements.txt
fi

if [ -n "$REDIS_PASSWORD" ]; then
    REDIS_PREFIX=:${REDIS_PASSWORD}@
else
    REDIS_PREFIX=
fi

wait_for_port() {
  local name="$1" host="$2" port="$3"
  local j=0
  while ! nc -z "$host" "$port" >/dev/null 2>&1 < /dev/null; do
    j=$((j+1))
    if [ $j -ge $TRY_LOOP ]; then
      echo >&2 "$(date) - $host:$port still not reachable, giving up"
      exit 1
    fi
    echo "$(date) - waiting for $name... $j/$TRY_LOOP"
    sleep 5
  done
}

if [ "$1" = "webserver" ] || [ "$1" = "worker" ] || [ "$1" = "scheduler" ] ; then
  wait_for_port "Postgres" "$POSTGRES_HOST" "$POSTGRES_PORT"
fi

AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql+psycopg2://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB"
AIRFLOW__CELERY__BROKER_URL="redis://$REDIS_PREFIX$REDIS_HOST:$REDIS_PORT/1"
AIRFLOW__CELERY__CELERY_RESULT_BACKEND="db+postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB"

# Update configuration depending the type of Executor
if [ "$AIRFLOW__CORE__EXECUTOR" = "CeleryExecutor" ]
then
  # Wait for Redis
  if [ "$1" = "webserver" ] || [ "$1" = "worker" ] || [ "$1" = "scheduler" ] || [ "$1" = "flower" ] ; then
    wait_for_port "Redis" "$REDIS_HOST" "$REDIS_PORT"
  fi
  if [ "$1" = "webserver" ]; then
    echo "Initialize database..."
    $CMD initdb
    exec "$CMD" webserver
  else
    sleep 10
    exec "$CMD" "$@"
  fi
elif [ "$AIRFLOW__CORE__EXECUTOR" = "LocalExecutor" ]
then
  echo "Initialize database..."
  $CMD initdb
  exec "$CMD" webserver &
  exec "$CMD" scheduler
# By default we use SequentialExecutor
else
  if [ "$1" = "version" ]; then
    exec "$CMD" version
    exit
  fi
  echo "Initialize database..."
  $CMD initdb
  exec "$CMD" webserver
fi
