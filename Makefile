.DEFAULT_GOAL := help
PYTHON ?= python3
VENV   ?= .venv
BIN     = $(VENV)/bin

.PHONY: help install dev lint format typecheck test test-all demo ui platform-up platform-up-spark platform-down platform-logs clean

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

$(BIN)/python:
	$(PYTHON) -m venv $(VENV)
	$(BIN)/pip install --upgrade pip

install: $(BIN)/python ## Install FinSight with the web UI
	$(BIN)/pip install -e ".[ui]"

dev: $(BIN)/python ## Install with all development tools and pre-commit hooks
	$(BIN)/pip install -e ".[dev]"
	$(BIN)/pre-commit install

lint: ## Lint and check formatting
	$(BIN)/ruff check src tests scripts platform
	$(BIN)/ruff format --check src tests scripts platform

format: ## Auto-format and fix lint issues
	$(BIN)/ruff format src tests scripts platform
	$(BIN)/ruff check --fix src tests scripts platform

typecheck: ## Type-check the library
	$(BIN)/mypy

test: ## Run the test suite (offline, no Spark)
	$(BIN)/pytest -m "not spark and not network"

test-all: ## Run every test, including Spark (needs Java 17/21)
	$(BIN)/pytest

demo: ## Fetch offline synthetic data and show the catalog
	$(BIN)/finsight fetch --universe mag7 --start 2024-01-01 --provider synthetic
	$(BIN)/finsight catalog

ui: ## Launch the web UI
	$(BIN)/finsight ui

platform-up: ## Start Airflow + MinIO (Option 2)
	docker compose up -d --build

platform-up-spark: ## Start Airflow + MinIO + Spark cluster
	docker compose --profile spark up -d --build

platform-down: ## Stop the platform (data volumes are kept)
	docker compose --profile spark down

platform-logs: ## Tail platform logs
	docker compose logs -f --tail=100

clean: ## Remove caches and build artifacts (keeps ./data)
	rm -rf .pytest_cache .ruff_cache .mypy_cache .coverage htmlcov build dist src/*.egg-info
	find . -name __pycache__ -type d -prune -not -path "./$(VENV)/*" -exec rm -rf {} +
