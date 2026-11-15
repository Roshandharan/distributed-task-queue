.PHONY: install test lint chaos bench up down k8s

install:
	pip install -e ".[dev]"

test:
	pytest

lint:
	ruff check . && ruff format --check .

chaos:
	python -m scripts.chaos --jobs 5000 --workers 6 --kill-every 0.4 --flush-redis

bench:
	python -m scripts.bench --jobs 20000 --workers 4 --concurrency 8

up:
	docker compose up --build --scale worker=3

down:
	docker compose down -v

k8s:
	kubectl apply -k k8s
