# Get Started

Build the container:

  docker build --rm -t puckel/docker-airflow .

Compose the container:

  docker-compose -f docker-compose-CeleryExecutor.yml up -d

You can also run the container after composing with,

  docker run -d -p 8080:8080 -e LOAD_EX=y puckel/docker-airflow
