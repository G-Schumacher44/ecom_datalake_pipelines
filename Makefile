.PHONY: lint format test airflow-init

lint:
	pylint src tests
	mypy src
	ruff check src tests

format:
	black src tests
	isort src tests

test:
	pytest -q

airflow-init:
	./scripts/bootstrap_airflow.sh
