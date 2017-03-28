#!/usr/bin/env bash

AIRFLOW_HOME="/usr/local/airflow"
CMD="airflow"
TRY_LOOP="10"
POSTGRES_HOST="postgres"
POSTGRES_PORT="5432"
RABBITMQ_HOST="rabbitmq"
RABBITMQ_CREDS="airflow:airflow"
: ${FERNET_KEY:=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print FERNET_KEY")}
# FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print FERNET_KEY")

# Load DAGs exemples (default: Yes)
if [ "x$LOAD_EX" = "xn" ]; then
    sed -i "s/load_examples = True/load_examples = False/" "$AIRFLOW_HOME"/airflow.cfg
fi

# Install custome python package if requirements.txt is present
if [ -e "/requirements.txt" ]; then
    $(which pip) install --user -r /requirements.txt
fi

# Give access to /var/run/docker.sock to aiflow user (its not always there)
sudo chown airflow:airflow /var/run/docker.sock | true

# Generate Fernet key
sed -i "s|\$FERNET_KEY|$FERNET_KEY|" "$AIRFLOW_HOME"/airflow.cfg

if [ "$1" = "version" ]; then
  exec $CMD version
fi

sed -i "s#sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@postgres/airflow#sql_alchemy_conn = sqlite:////usr/local/airflow/airflow.db#" "$AIRFLOW_HOME"/airflow.cfg
echo "Initialize database..."
$CMD initdb
echo "Database initialized!"

python3 -m pytest ./tests
