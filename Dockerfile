# VERSION 1.10.11
# AUTHOR: Swapnil Gusani
# DESCRIPTION: Basic Airflow container
# BUILD: docker build --rm -t swapniel99/docker-airflow .
# SOURCE: https://github.com/swapniel99/docker-airflow

FROM apache/airflow:1.10.11

USER root

##System upgrade
#RUN set -ex \
#    # Upgrade packages
#    && apt-get update -yqq \
#    && apt-get upgrade -yqq \
#    && pip install --upgrade pip \
#    # Cleanup
#    && apt-get autoremove -yqq --purge \
#    && apt-get clean \
#    && rm -rf \
#        /var/lib/apt/lists/* \
#        /root/.cache/pip \
#        /tmp/* \
#        /var/tmp/* \
#        /usr/share/man \
#        /usr/share/doc \
#        /usr/share/doc-base

# Copy Config Files
COPY script/entrypoint_wrapper.sh /entrypoint_wrapper.sh
COPY config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

# Make airflow user owner
RUN chown -R airflow: ${AIRFLOW_HOME}

EXPOSE 5555 8793

USER airflow
ENTRYPOINT ["/entrypoint_wrapper.sh"]
