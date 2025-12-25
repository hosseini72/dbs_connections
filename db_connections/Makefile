.PHONY: help install install-dev test test-verbose test-coverage lint format type-check clean build publish docs

help:
	@echo "Available commands:"
	@echo "  install       Install package"
	@echo "  install-dev   Install package with dev dependencies"
	@echo "  test          Run tests with unittest"
	@echo "  test-verbose  Run tests with verbose output"
	@echo "  test-coverage Run tests with coverage report"
	@echo "  lint          Run linters"
	@echo "  format        Format code"
	@echo "  type-check    Run type checker"
	@echo "  clean         Clean build artifacts"
	@echo "  build         Build package"
	@echo "  publish       Publish to PyPI"
	@echo "  docs          Build documentation"

install:
	pip install -e .

install-dev:
	pip install -e ".[dev,all]"

test:
	python -m unittest discover -s tests/unit/connectors -p "test_*.py"
	@if [ -f tests/unit/test_core.py ]; then \
		python -m unittest tests.unit.test_core; \
	fi

test-verbose:
	python -m unittest discover -s tests/unit/connectors -p "test_*.py" -v
	@if [ -f tests/unit/test_core.py ]; then \
		python -m unittest tests.unit.test_core -v; \
	fi

test-coverage:
	coverage run -m unittest discover -s tests/unit/connectors -p "test_*.py"
	@if [ -f tests/unit/test_core.py ]; then \
		coverage run -m unittest tests.unit.test_core; \
	fi
	coverage report
	coverage html

lint:
	ruff check scr/ tests/

format:
	black scr/ tests/
	ruff check --fix scr/ tests/

type-check:
	mypy scr/

clean:
	rm -rf build/ dist/ *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage

build: clean
	python -m build

publish: build
	python -m twine upload dist/*

docs:
	cd docs && make html

