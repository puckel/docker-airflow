#!/usr/bin/env bash

AIRFLOW_HOME="/usr/local/airflow"
CMD="airflow"
TRY_LOOP="10"
RABBITMQ_HOST="rabbitmq"
RABBITMQ_CREDS="airflow:airflow"
: ${FERNET_KEY:=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print FERNET_KEY")}

# Load DAGs exemples (default: Yes)
if [ "x$LOAD_EX" = "xn" ]; then
    sed -i "s/load_examples = True/load_examples = False/" "$AIRFLOW_HOME"/airflow.cfg
fi

# Install custome python package if requirements.txt is present
if [ -e "/requirements.txt" ]; then
    $(which pip) install --user -r /requirements.txt
fi

# Generate Fernet key
sed -i "s|\$FERNET_KEY|$FERNET_KEY|" "$AIRFLOW_HOME"/airflow.cfg

# wait for DB
if [ "$1" = "webserver" ] || [ "$1" = "worker" ] || [ "$1" = "scheduler" ] ; then
  i=0
  while ! /test_conn; do
    i=$((i+1))
    if [ $i -ge $TRY_LOOP ]; then
      echo "$(date) - ${AIRFLOW__CORE__SQL_ALCHEMY_CONN} still not reachable, giving up"
      exit 1
    fi
    echo "$(date) - waiting for ${AIRFLOW__CORE__SQL_ALCHEMY_CONN}... $i/$TRY_LOOP"
    sleep 5
  done
  if [ "$1" = "webserver" ]; then
    echo "Initialize database..."
    $CMD initdb
  fi
  sleep 5
fi

# If we use docker-compose, we use Celery (rabbitmq container).
if [ "x$EXECUTOR" = "xCelery" ]
then
# wait for rabbitmq
  if [ "$1" = "webserver" ] || [ "$1" = "worker" ] || [ "$1" = "scheduler" ] || [ "$1" = "flower" ] ; then
    j=0
    while ! curl -sI -u $RABBITMQ_CREDS http://$RABBITMQ_HOST:15672/api/whoami |grep '200 OK'; do
      j=$((j+1))
      if [ $j -ge $TRY_LOOP ]; then
        echo "$(date) - $RABBITMQ_HOST still not reachable, giving up"
        exit 1
      fi
      echo "$(date) - waiting for RabbitMQ... $j/$TRY_LOOP"
      sleep 5
    done
  fi
  exec $CMD "$@"
elif [ "x$EXECUTOR" = "xLocal" ]
then
  sed -i "s/executor = CeleryExecutor/executor = LocalExecutor/" "$AIRFLOW_HOME"/airflow.cfg
  exec $CMD "$@"
else
  if [ "$1" = "version" ]; then
    exec $CMD version
  fi
  sed -i "s/executor = CeleryExecutor/executor = SequentialExecutor/" "$AIRFLOW_HOME"/airflow.cfg
  sed -i "s#sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@postgres/airflow#sql_alchemy_conn = sqlite:////usr/local/airflow/airflow.db#" "$AIRFLOW_HOME"/airflow.cfg
  echo "Initialize database..."
  $CMD initdb
  exec $CMD webserver
fi
