.PHONY: help install install-dev format lint type-check test test-cov clean setup pre-commit run-all

help:  ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install:  ## Install project dependencies
	uv pip install -r requirements.txt

install-dev:  ## Install project + development dependencies
	uv pip install -e ".[dev]"
	pre-commit install

setup:  ## Complete setup (venv + deps + pre-commit)
	@echo "🚀 Setting up development environment..."
	python -m venv .venv
	@echo "✓ Virtual environment created"
	@echo "📦 Installing dependencies..."
	@bash -c "source .venv/bin/activate && uv pip install -e '.[dev]'"
	@echo "✓ Dependencies installed"
	@echo "🪝 Installing pre-commit hooks..."
	@bash -c "source .venv/bin/activate && pre-commit install"
	@echo "✓ Pre-commit hooks installed"
	@echo ""
	@echo "✅ Setup complete! Activate with: source .venv/bin/activate"

format:  ## Format code with black and ruff
	@echo "🎨 Formatting code..."
	black jobs/ utils/ *.py
	ruff check --fix jobs/ utils/ *.py
	@echo "✓ Formatting complete"

lint:  ## Lint code with ruff
	@echo "🔍 Linting code..."
	ruff check jobs/ utils/ *.py
	@echo "✓ Linting complete"

type-check:  ## Run type checking with mypy
	@echo "🔎 Type checking..."
	mypy jobs/ utils/ *.py
	@echo "✓ Type checking complete"

test:  ## Run tests with pytest
	@echo "🧪 Running tests..."
	pytest tests/ -v
	@echo "✓ Tests complete"

test-cov:  ## Run tests with coverage
	@echo "🧪 Running tests with coverage..."
	pytest tests/ -v --cov --cov-report=html --cov-report=term
	@echo "✓ Coverage report generated in htmlcov/"

check:  ## Run all checks (format, lint, type-check)
	@echo "🔍 Running all checks..."
	@make format
	@make lint
	@make type-check
	@echo "✅ All checks passed!"

pre-commit:  ## Run pre-commit on all files
	@echo "🪝 Running pre-commit hooks..."
	pre-commit run --all-files

clean:  ## Clean up generated files
	@echo "🧹 Cleaning up..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.coverage" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	rm -rf build/ dist/
	@echo "✓ Cleanup complete"

run-job-1:  ## Run Job 1: DataFrame Basics
	python jobs/01_dataframe_basics.py

run-job-2:  ## Run Job 2: Aggregations
	python jobs/02_aggregations.py

run-job-3:  ## Run Job 3: Joins
	python jobs/03_joins.py

run-job-4:  ## Run Job 4: Analytics & UDFs
	python jobs/04_analytics_udfs.py

run-job-5:  ## Run Job 5: Search Indexing
	python jobs/05_search_indexing.py

run-all:  ## Run all jobs in sequence (non-interactive)
	python run_all_jobs.py -y

jupyter:  ## Start Jupyter notebook server
	python start_jupyter.py

verify:  ## Verify environment setup
	python verify_setup.py
