#!/usr/bin/env bash

AIRFLOW_HOME="/usr/local/airflow"
CMD="airflow"
TRY_LOOP="20"

# Load DAGs exemples (default: Yes)
if [ "$POSTGRES_HOST_PORT" = "postgres" ]; then
    POSTGRES_HOST=$POSTGRES_HOST_PORT
    POSTGRES_PORT=5432
else
    POSTGRES_HOST=$(echo $POSTGRES_HOST_PORT | cut -f1 -d:)
    POSTGRES_PORT=$(echo $POSTGRES_HOST_PORT | cut -f2 -d:)
fi

# Load DAGs exemples (default: Yes)
if [ "$LOAD_EX" = "y" ]; then
    sed -i "s/load_examples = False/load_examples = True/" "$AIRFLOW_HOME"/airflow.cfg
fi

# Replace SMTP username and password
if [ -n "$SMTP_USER" ] || [ -n "$SMTP_PASSWORD" ]; then
    echo "Configuring SMTP username and password"
    sed -i "s/\$SMTP_USER/$SMTP_USER/" "$AIRFLOW_HOME"/airflow.cfg
    sed -i "s/\$SMTP_PASSWORD/$SMTP_PASSWORD/" "$AIRFLOW_HOME"/airflow.cfg
fi

# Update airflow config - Fernet key
sed -i "s|\$FERNET_KEY|$FERNET_KEY|" "$AIRFLOW_HOME"/airflow.cfg

if [ -n "$REDIS_PASSWORD" ]; then
    REDIS_PREFIX=:${REDIS_PASSWORD}@
else
    REDIS_PREFIX=
fi

# Wait for Postresql
if [ "$1" = "webserver" ] || [ "$1" = "worker" ] || [ "$1" = "scheduler" ] ; then
  i=0
  while ! nc -z $POSTGRES_HOST $POSTGRES_PORT >/dev/null 2>&1 < /dev/null; do
    i=$((i+1))
    if [ "$1" = "webserver" ]; then
      echo "$(date) - waiting for ${POSTGRES_HOST}:${POSTGRES_PORT}... $i/$TRY_LOOP"
      if [ $i -ge $TRY_LOOP ]; then
        echo "$(date) - ${POSTGRES_HOST}:${POSTGRES_PORT} still not reachable, giving up"
        exit 1
      fi
    fi
    sleep 10
  done
fi

# git clone 
if [ "$1" = "webserver" ] || [ "$1" = "worker" ] || [ "$1" = "scheduler" ] ; then
    if [ -d "${AIRFLOW_HOME}/priest" ] ; then
	cd ${AIRFLOW_HOME}/priest && git pull https://${GIT_KEY}@github.com/bellhops/priest master && cd -
    else
	git clone https://${GIT_KEY}@github.com/bellhops/priest ${AIRFLOW_HOME}/priest
	cp -R ${AIRFLOW_HOME}/priest/dags ${AIRFLOW_HOME}/dags
    fi
fi

# Update configuration depending the type of Executor
if [ "$EXECUTOR" = "Celery" ]
then
  # Wait for Redis
  if [ "$1" = "webserver" ] || [ "$1" = "worker" ] || [ "$1" = "scheduler" ] || [ "$1" = "flower" ] ; then
    j=0
    while ! nc -z $REDIS_HOST $REDIS_PORT >/dev/null 2>&1 < /dev/null; do
      j=$((j+1))
      if [ $j -ge $TRY_LOOP ]; then
        echo "$(date) - $REDIS_HOST still not reachable, giving up"
        exit 1
      fi
      echo "$(date) - waiting for Redis... $j/$TRY_LOOP"
      sleep 5
    done
  fi
  sed -i "s#celery_result_backend = db+postgresql://\$POSTGRES_USER:\$POSTGRES_PASSWORD@\$POSTGRES_HOST_PORT/\$POSTGRES_DB#celery_result_backend = db+postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB#" "$AIRFLOW_HOME"/airflow.cfg
  sed -i "s#sql_alchemy_conn = postgresql+psycopg2://\$POSTGRES_USER:\$POSTGRES_PASSWORD@\$POSTGRES_HOST_PORT/\$POSTGRES_DB#sql_alchemy_conn = postgresql+psycopg2://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB#" "$AIRFLOW_HOME"/airflow.cfg
  sed -i "s#broker_url = redis://\$REDIS_HOST:\$REDIS_PORT/\$REDIS_QUEUE#broker_url = redis://$REDIS_HOST:$REDIS_PORT/$REDIS_QUEUE#" "$AIRFLOW_HOME"/airflow.cfg
  if [ "$1" = "webserver" ]; then
    echo "Initialize database..."
    $CMD initdb
    exec $CMD webserver
  else
    sleep 10
    exec $CMD "$@"
  fi
elif [ "$EXECUTOR" = "Local" ]
then
  sed -i "s/executor = CeleryExecutor/executor = LocalExecutor/" "$AIRFLOW_HOME"/airflow.cfg
  sed -i "s#sql_alchemy_conn = postgresql+psycopg2://\$POSTGRES_USER:\$POSTGRES_PASSWORD@\$POSTGRES_HOST_PORT/\$POSTGRES_DB#sql_alchemy_conn = postgresql+psycopg2://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB#" "$AIRFLOW_HOME"/airflow.cfg
  sed -i "s#broker_url = redis://\$REDIS_HOST:\$REDIS_PORT/\$REDIS_QUEUE#broker_url = redis://$REDIS_HOST:$REDIS_PORT/$REDIS_QUEUE#" "$AIRFLOW_HOME"/airflow.cfg
  echo "Initialize database..."
  $CMD initdb
  exec $CMD webserver &
  exec $CMD scheduler
# By default we use SequentialExecutor
else
  if [ "$1" = "version" ]; then
    exec $CMD version
    exit
  fi
  sed -i "s/executor = CeleryExecutor/executor = SequentialExecutor/" "$AIRFLOW_HOME"/airflow.cfg
  sed -i "s#sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@postgres/airflow#sql_alchemy_conn = sqlite:////usr/local/airflow/airflow.db#" "$AIRFLOW_HOME"/airflow.cfg
  echo "Initialize database..."
  $CMD initdb
  exec $CMD webserver
fi
