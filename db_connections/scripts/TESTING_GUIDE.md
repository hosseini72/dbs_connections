# Testing Guide

This guide explains how to run tests using the provided scripts and commands.

## Prerequisites

1. **Python 3.8+** installed
2. **Docker** (for integration tests)
3. **Make** (optional, for Makefile commands)
4. **Git Bash** or **WSL** (for bash scripts on Windows)

## Quick Start

### 1. Setup Test Environment

**Bash (Linux/Mac/Git Bash):**
```bash
./scripts/setup_test_env.sh
```

**PowerShell (Windows):**
```powershell
# Start Docker containers manually
docker-compose -f tests/unit/integration/docker-compose.yml up -d

# Install test dependencies
pip install -e ".[postgres,redis,mongodb,clickhouse,rabbitmq,neo4j]"
```

### 2. Run Tests

**Option A: Using Make (if available)**
```bash
make test              # Run all tests
make test-verbose      # Run with verbose output
make test-coverage     # Run with coverage report
```

**Option B: Using Bash Scripts (Linux/Mac/Git Bash)**
```bash
# Run all tests
./scripts/run_tests.sh all

# Run specific test types
./scripts/run_tests.sh unit          # Unit tests only
./scripts/run_tests.sh integration   # Integration tests
./scripts/run_tests.sh postgres      # PostgreSQL tests
./scripts/run_tests.sh coverage      # With coverage

# With verbose output
./scripts/run_tests.sh unit -v
```

**Option C: Direct Python Commands (Works everywhere)**
```bash
# Run all tests
python -m unittest discover -s tests -p "test_*.py"

# Run with verbose output
python -m unittest discover -s tests -p "test_*.py" -v

# Run unit tests only
python -m unittest discover -s tests/unit -p "test_*.py" -v

# Run integration tests
python -m unittest discover -s tests/unit/integration -p "test_*.py" -v

# Run specific test file
python -m unittest tests.unit.connectors.postgres.test_postgres_config -v
```

## Windows PowerShell Commands

Since you're on Windows, here are PowerShell equivalents:

### Setup Test Environment (PowerShell)

```powershell
# Start Docker containers
docker-compose -f tests/unit/integration/docker-compose.yml up -d

# Wait for services
Start-Sleep -Seconds 10

# Install dependencies
pip install -e ".[postgres,redis,mongodb,clickhouse,rabbitmq,neo4j]"
```

### Run Tests (PowerShell)

**Option 1: Using PowerShell Script (Recommended)**
```powershell
# Run all tests
.\scripts\run_tests.ps1 all

# Run unit tests
.\scripts\run_tests.ps1 unit

# Run with verbose output
.\scripts\run_tests.ps1 unit -Verbose

# Run integration tests
.\scripts\run_tests.ps1 integration

# Run PostgreSQL tests
.\scripts\run_tests.ps1 postgres

# Run with coverage
.\scripts\run_tests.ps1 coverage

# Show help
.\scripts\run_tests.ps1 help
```

**Option 2: Direct Python Commands**
```powershell
# Run all tests
python -m unittest discover -s tests -p "test_*.py"

# Run with verbose output
python -m unittest discover -s tests -p "test_*.py" -v

# Run unit tests (excludes integration tests)
python -m unittest discover -s tests/unit/connectors -p "test_*.py" -v
python -m unittest tests.unit.test_core -p "test_*.py" -v

# Run integration tests
python -m unittest discover -s tests/unit/integration -p "test_*.py" -v

# Run PostgreSQL tests
python -m unittest discover -s tests/unit/connectors/postgres -p "test_*.py" -v

# Run with coverage
coverage run -m unittest discover -s tests -p "test_*.py"
coverage report
coverage html
```

**Note:** In PowerShell, you must use `.\` (dot-backslash) to run scripts from the current directory. This is a PowerShell security feature.

## Test Types

### Unit Tests
Fast tests that don't require external services:
```bash
python -m unittest discover -s tests/unit -p "test_*.py" -v
```

### Integration Tests
Tests that require Docker containers:
```bash
# Start containers first
docker-compose -f tests/unit/integration/docker-compose.yml up -d

# Run integration tests
python -m unittest discover -s tests/unit/integration -p "test_*.py" -v
```

### Database-Specific Tests

```bash
# PostgreSQL
python -m unittest discover -s tests/unit/connectors/postgres -p "test_*.py" -v

# Redis
python -m unittest discover -s tests/unit/connectors/redis -p "test_*.py" -v

# MongoDB
python -m unittest discover -s tests/unit/connectors/mongodb -p "test_*.py" -v

# ClickHouse
python -m unittest discover -s tests/unit/connectors/clickhouse -p "test_*.py" -v

# RabbitMQ
python -m unittest discover -s tests/unit/connectors/rabbitmq -p "test_*.py" -v

# Neo4j
python -m unittest discover -s tests/unit/connectors/neo4j -p "test_*.py" -v
```

### Configuration Tests
```bash
python -m unittest discover -s tests/unit/connectors -p "test_*_config.py" -v
```

### Health Check Tests
```bash
python -m unittest discover -s tests/unit/connectors -p "test_*_health.py" -v
```

## Coverage Reports

```bash
# Install coverage if needed
pip install coverage

# Run tests with coverage
coverage run -m unittest discover -s tests -p "test_*.py"

# View terminal report
coverage report

# Generate HTML report
coverage html

# Open HTML report (Windows)
start htmlcov/index.html

# Open HTML report (Linux/Mac)
open htmlcov/index.html  # Mac
xdg-open htmlcov/index.html  # Linux
```

## Cleanup

**Bash:**
```bash
./scripts/teardown_test_env.sh
```

**PowerShell:**
```powershell
# Stop Docker containers
docker-compose -f tests/unit/integration/docker-compose.yml down

# Clean test artifacts
Remove-Item -Recurse -Force htmlcov, .coverage, coverage.xml -ErrorAction SilentlyContinue
Get-ChildItem -Recurse -Filter "__pycache__" | Remove-Item -Recurse -Force
Get-ChildItem -Recurse -Filter "*.pyc" | Remove-Item -Force
```

## Troubleshooting

### Import Errors
If you get `ModuleNotFoundError: No module named 'db_connections'`:

1. Make sure you're in the project root directory
2. Install the package in development mode:
   ```bash
   pip install -e .
   ```

### Docker Not Running
If integration tests fail:
```bash
# Check if Docker is running
docker ps

# Start test containers
docker-compose -f tests/unit/integration/docker-compose.yml up -d
```

### Permission Denied (Bash Scripts)
```bash
# Make scripts executable
chmod +x scripts/*.sh
```

## Examples

### Run All Tests
```bash
python -m unittest discover -s tests -p "test_*.py" -v
```

### Run Single Test File
```bash
python -m unittest tests.unit.connectors.postgres.test_postgres_config -v
```

### Run Single Test Class
```bash
python -m unittest tests.unit.connectors.postgres.test_postgres_config.TestPostgresPoolConfig -v
```

### Run Single Test Method
```bash
python -m unittest tests.unit.connectors.postgres.test_postgres_config.TestPostgresPoolConfig.test_config_creation -v
```

## Summary

**For Windows (PowerShell):**
- Use direct Python commands: `python -m unittest discover ...`
- Or create PowerShell scripts (`.ps1` files)

**For Linux/Mac/Git Bash:**
- Use bash scripts: `./scripts/run_tests.sh`
- Or use Make: `make test`
- Or use direct Python commands

**All Platforms:**
- Direct Python commands work everywhere
- Make commands work if Make is installed
- Docker commands work if Docker is installed

