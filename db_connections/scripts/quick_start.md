# Quick Start Guide

## Setup Test Environment

**Bash (Linux/Mac/Git Bash):**
```bash
# Setup test environment (starts Docker containers)
./scripts/setup_test_env.sh
```

**PowerShell (Windows):**
```powershell
# Start Docker containers
docker-compose -f tests/unit/integration/docker-compose.yml up -d

# Install test dependencies
pip install -e ".[postgres,redis,mongodb,clickhouse,rabbitmq,neo4j]"
```

## Running Tests

### Using Make Commands (if Make is installed)

```bash
# Run all tests
make test

# Run with verbose output
make test-verbose

# Run with coverage report
make test-coverage
```

### Using Test Scripts

**Bash Script (Linux/Mac/Git Bash):**
```bash
# Run all tests
./scripts/run_tests.sh all

# Run specific test types
./scripts/run_tests.sh unit          # Unit tests only
./scripts/run_tests.sh integration   # Integration tests
./scripts/run_tests.sh postgres      # PostgreSQL-specific
./scripts/run_tests.sh redis         # Redis-specific
./scripts/run_tests.sh mongodb       # MongoDB-specific
./scripts/run_tests.sh clickhouse    # ClickHouse-specific
./scripts/run_tests.sh rabbitmq      # RabbitMQ-specific
./scripts/run_tests.sh neo4j         # Neo4j-specific
./scripts/run_tests.sh config        # Configuration tests
./scripts/run_tests.sh health        # Health check tests
./scripts/run_tests.sh coverage      # With coverage report

# With verbose output
./scripts/run_tests.sh unit -v
```

**PowerShell Script (Windows):**
```powershell
# IMPORTANT: Use .\ (dot-backslash) to run scripts from current directory
# This is required by PowerShell for security reasons

# Run all tests
.\scripts\run_tests.ps1 all

# Run specific test types
.\scripts\run_tests.ps1 unit          # Unit tests only
.\scripts\run_tests.ps1 integration   # Integration tests
.\scripts\run_tests.ps1 postgres      # PostgreSQL-specific
.\scripts\run_tests.ps1 redis         # Redis-specific
.\scripts\run_tests.ps1 mongodb       # MongoDB-specific
.\scripts\run_tests.ps1 clickhouse    # ClickHouse-specific
.\scripts\run_tests.ps1 rabbitmq      # RabbitMQ-specific
.\scripts\run_tests.ps1 neo4j         # Neo4j-specific
.\scripts\run_tests.ps1 config        # Configuration tests
.\scripts\run_tests.ps1 health        # Health check tests
.\scripts\run_tests.ps1 coverage      # With coverage report

# With verbose output
.\scripts\run_tests.ps1 unit -Verbose

# Show help
.\scripts\run_tests.ps1 help
```

**Note:** If you get an execution policy error, you may need to allow script execution:
```powershell
# Check current policy
Get-ExecutionPolicy

# Allow scripts for current user (if needed)
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### Direct unittest Commands

```bash
# Run all tests
python -m unittest discover -s tests -p "test_*.py"

# Run with verbose output
python -m unittest discover -s tests -p "test_*.py" -v

# Run unit tests only (excludes integration tests)
python -m unittest discover -s tests/unit/connectors -p "test_*.py" -v
python -m unittest tests.unit.test_core -p "test_*.py" -v

# Run integration tests (requires Docker)
python -m unittest discover -s tests/unit/integration -p "test_*.py" -v

# Run specific test module
python -m unittest tests.unit.connectors.postgres.test_postgres_config -v

# Run specific test class
python -m unittest tests.unit.connectors.postgres.test_postgres_config.TestPostgresPoolConfig -v

# Run specific test method
python -m unittest tests.unit.connectors.postgres.test_postgres_config.TestPostgresPoolConfig.test_config_creation -v
```

### Running Tests with Coverage

```bash
# Install coverage if not already installed
pip install coverage

# Run tests with coverage
coverage run -m unittest discover -s tests -p "test_*.py"

# Generate coverage report
coverage report

# Generate HTML coverage report
coverage html
# Open htmlcov/index.html in your browser
```

## Docker Commands

### Start Test Databases

```bash
# Start all test databases
docker-compose -f tests/unit/integration/docker-compose.yml up -d

# Check status
docker-compose -f tests/unit/integration/docker-compose.yml ps

# View logs
docker-compose -f tests/unit/integration/docker-compose.yml logs -f
```

### Stop Test Databases

```bash
# Stop databases
docker-compose -f tests/unit/integration/docker-compose.yml down

# Stop and remove volumes (deletes all test data)
docker-compose -f tests/unit/integration/docker-compose.yml down -v
```

## Cleanup

```bash
# Teardown test environment
./scripts/teardown_test_env.sh
```

## Test Organization

- `tests/unit/` - Unit tests (fast, no external dependencies)
- `tests/unit/connectors/` - Database connector tests
- `tests/unit/integration/` - Integration tests (require external services)

## Notes

- Unittest is part of Python's standard library (no external dependencies needed)
- For async tests, use `unittest.IsolatedAsyncioTestCase` (Python 3.8+)
- Integration tests require Docker containers to be running
- Coverage reports are generated in `htmlcov/` directory
