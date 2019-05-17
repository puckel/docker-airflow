include .env

.PHONY: build

build:
	docker build -t ${NAME}:${VERSION} .

push:
	docker push ${NAME}:${VERSION}
