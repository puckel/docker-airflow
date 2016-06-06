#!/usr/bin/env bash

CMD="airflow"
TRY_LOOP="10"
POSTGRES_HOST="postgres"
POSTGRES_PORT="5432"
RABBITMQ_HOST="rabbitmq"
RABBITMQ_CREDS="airflow:airflow"
FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print FERNET_KEY")

# Generate Fernet key
sed -i "s/{FERNET_KEY}/${FERNET_KEY}/" $AIRFLOW_HOME/airflow.cfg

# wait for rabbitmq
if [ "$1" = "webserver" ] || [ "$1" = "worker" ] || [ "$1" = "scheduler" ] || [ "$1" = "flower" ] ; then
  j=0
  while ! curl -sI -u $RABBITMQ_CREDS http://$RABBITMQ_HOST:15672/api/whoami |grep '200 OK'; do
    j=`expr $j + 1`
    if [ $j -ge $TRY_LOOP ]; then
      echo "$(date) - $RABBITMQ_HOST still not reachable, giving up"
      exit 1
    fi
    echo "$(date) - waiting for RabbitMQ... $j/$TRY_LOOP"
    sleep 5
  done
fi

# wait for DB
if [ "$1" = "webserver" ] || [ "$1" = "worker" ] || [ "$1" = "scheduler" ] ; then
  i=0
  while ! nc $POSTGRES_HOST $POSTGRES_PORT >/dev/null 2>&1 < /dev/null; do
    i=`expr $i + 1`
    if [ $i -ge $TRY_LOOP ]; then
      echo "$(date) - ${POSTGRES_HOST}:${POSTGRES_PORT} still not reachable, giving up"
      exit 1
    fi
    echo "$(date) - waiting for ${POSTGRES_HOST}:${POSTGRES_PORT}... $i/$TRY_LOOP"
    sleep 5
  done
  if [ "$1" = "webserver" ]; then
    echo "Initialize database..."
    $CMD initdb
  fi
  sleep 5
fi

exec $CMD "$@"
