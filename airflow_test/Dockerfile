FROM fathomhealth/docker-airflow

COPY requirements.txt /usr/local/airflow
COPY test_entrypoint.sh /test_entrypoint.sh

RUN sudo pip3 install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=:/usr/src/app/diseaseTools
ENTRYPOINT ["/test_entrypoint.sh"]
