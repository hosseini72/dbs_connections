# MyCompany Database Connection Management

A unified Python library for managing database connections across multiple Database Management Systems (DBMSs).

## Features

- 🔌 **Multiple DBMS Support**: PostgreSQL, Redis, ClickHouse, MongoDB, RabbitMQ, Neo4j
- ⚡ **Async & Sync**: Both async and synchronous connection pool implementations
- 🔄 **Connection Pooling**: Efficient connection reuse with configurable pool sizes
- 🏥 **Health Checks**: Built-in health monitoring for all connections
- 📊 **Metrics**: Track pool usage and performance
- 🔧 **Framework Integration**: Ready-to-use middleware for FastAPI, Flask, and Django
- 🎯 **Type Safety**: Full type hints for better IDE support
- 🧪 **Well Tested**: Comprehensive test coverage

## Installation

```bash
# Basic installation
pip install mycompany-db

# With specific database support
pip install mycompany-db[postgres]
pip install mycompany-db[redis]
pip install mycompany-db[mongodb]

# With all databases
pip install mycompany-db[all]

# For development
pip install mycompany-db[dev]
```

## Quick Start

```python
from mycompany_db.connectors.postgres import PostgresPool
from mycompany_db.core.config import BasePoolConfig

# Create a pool configuration
config = BasePoolConfig(
    max_size=10,
    min_size=2,
    timeout=30,
)

# Initialize the pool
pool = PostgresPool(
    host="localhost",
    port=5432,
    database="mydb",
    user="user",
    password="password",
    config=config
)

# Use the pool
async with pool:
    async with pool.get_connection() as conn:
        result = await conn.execute("SELECT * FROM users")
```

## Documentation

Full documentation is available at [https://mycompany-db.readthedocs.io](https://mycompany-db.readthedocs.io)

## Contributing

Contributions are welcome! Please read our contributing guidelines first.

## License

MIT License - see LICENSE file for details


# ============================================================================
# Makefile
# ============================================================================
.PHONY: help install install-dev test lint format type-check clean build publish docs

help:
	@echo "Available commands:"
	@echo "  install       Install package"
	@echo "  install-dev   Install package with dev dependencies"
	@echo "  test          Run tests"
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
	pytest

lint:
	ruff check src/ tests/

format:
	black src/ tests/
	ruff check --fix src/ tests/

type-check:
	mypy src/

clean:
	rm -rf build/ dist/ *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov

build: clean
	python -m build

publish: build
	python -m twine upload dist/*

docs:
	cd docs && make html
