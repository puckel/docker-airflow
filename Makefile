SERVICE = "scheduler"
TITLE = "Airflow Containers"
FILE = "docker-compose-LocalExecutor.yml"

.PHONY: run

build:
	docker-compose -f $(FILE)  build

up:
	@echo "Starting $(TITLE)"
	docker-compose -f $(FILE)  up -d

upf:
	@echo "Starting $(TITLE)"
	docker-compose -f $(FILE)  up

down:
	@echo "Stopping $(TITLE)"
	docker-compose -f $(FILE)  down

restart:
	@echo "Restarting $(TITLE)"
	docker-compose -f $(FILE)  restart

downup: down print-newline up

run:
	docker-compose -f $(FILE)  run --rm --entrypoint='' $(SERVICE) bash

runr:
	docker-compose -f $(FILE)  run --rm --entrypoint='' -u root $(SERVICE) bash

bash:
	docker-compose -f $(FILE)  exec $(SERVICE) bash

bashr:
	docker-compose -f $(FILE)  exec -u root $(SERVICE) bash

logs:
	docker-compose -f $(FILE)  logs --tail 50 --follow $(SERVICE)

conf:
	docker-compose -f $(FILE)  config

initdb:
	docker-compose -f $(FILE)  run --rm $(SERVICE) initdb

upgradedb:
	docker-compose -f $(FILE)  run --rm $(SERVICE) upgradedb

resetdb:
	docker-compose -f $(FILE)  run --rm $(SERVICE) resetdb

print-newline:
	@echo ""
	@echo ""
