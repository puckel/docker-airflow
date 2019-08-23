NAME = ryan/airflow
VERSION = 1.10.4

.PHONY: build start push

build:
	docker build -t ${NAME}:${VERSION}  .

start:
	docker run -it --rm ${NAME}:${VERSION} /bin/bash

push:
	docker push ${NAME}:${VERSION}
