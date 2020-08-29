SERVICE = "scheduler"
TITLE = "Airflow Containers"
FILE = "docker-compose-LocalExecutor.yml"

.PHONY: run

build:
	docker build -t docker-airflow .
	docker tag docker-airflow-ascent:latest swapniel99/docker-airflow:latest

up:
	@echo "Starting $(TITLE)"
	docker-compose -f $(FILE) up -d

upf:
	@echo "Starting $(TITLE)"
	docker-compose -f $(FILE) up

down:
	@echo "Stopping $(TITLE)"
	docker-compose -f $(FILE) down

start:
	@echo "Starting $(TITLE)"
	docker-compose -f $(FILE) start

stop:
	@echo "Stopping $(TITLE)"
	docker-compose -f $(FILE) stop

restart:
	@echo "Restarting $(TITLE)"
	docker-compose -f $(FILE) restart

downup: down print-newline up

run:
	docker-compose -f $(FILE) run --rm --entrypoint='' $(SERVICE) bash

runr:
	docker-compose -f $(FILE) run --rm --entrypoint='' -u root $(SERVICE) bash

bash:
	docker-compose -f $(FILE) exec $(SERVICE) bash

bashr:
	docker-compose -f $(FILE) exec -u root $(SERVICE) bash

logs:
	docker-compose -f $(FILE) logs --tail 50 --follow $(SERVICE)

conf:
	docker-compose -f $(FILE) config

initdb:
	docker-compose -f $(FILE) run --rm $(SERVICE) initdb

upgradedb:
	docker-compose -f $(FILE) run --rm $(SERVICE) upgradedb

resetdb:
	docker-compose -f $(FILE) run --rm $(SERVICE) resetdb

rbacadmin:
	# Change user details and password after login if using RBAC mode.
	docker-compose -f $(FILE) exec webserver airflow create_user -r Admin -u admin -e admin@example.com -f Firstname -l Lastname -p admin123

print-newline:
	@echo ""
	@echo ""
