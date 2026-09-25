.PHONY: up down seed verify logs reset

BASH ?= bash
ifeq ($(OS),Windows_NT)
  ifneq ($(wildcard C:/Program Files/Git/bin/bash.exe),)
    BASH := "C:/Program Files/Git/bin/bash.exe"
  endif
endif

up:
	docker compose up -d --build

down:
	docker compose down

reset:
	docker compose down -v

seed:
	$(BASH) scripts/seed.sh

verify:
	$(BASH) verify.sh

logs:
	docker compose logs -f --tail=100 pipeline consumer
