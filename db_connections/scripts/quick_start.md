# Setup test environment
./scripts/setup_test_env.sh

# Run all tests
make test

# Run specific test types
make test-unit          # Unit tests only
make test-integration   # Integration tests
make test-postgres      # PostgreSQL-specific
make test-coverage      # With coverage report
make test-fast          # Exclude slow tests

# Cleanup
make teardown-test


Direct pytest Commands

# Unit tests
pytest tests/unit -m unit

# Integration tests (requires Docker)
pytest tests/integration -m integration

# PostgreSQL tests only
pytest tests/ -m postgres

# With coverage
pytest tests/ --cov=src/mycompany_db --cov-report=html

# Verbose output
pytest tests/ -vv

# Specific test file
pytest tests/unit/connectors/test_postgres_pool.py


# Start all test databases
docker-compose -f tests/integration/docker-compose.yml up -d

# Check status
docker-compose -f tests/integration/docker-compose.yml ps

# Stop databases
docker-compose -f tests/integration/docker-compose.yml down