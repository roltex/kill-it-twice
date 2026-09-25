.PHONY: up down seed verify logs reset

up:
	docker compose up -d --build

down:
	docker compose down

reset:
	docker compose down -v

seed:
	bash scripts/seed.sh

verify:
	bash verify.sh

logs:
	docker compose logs -f --tail=100 pipeline consumer
