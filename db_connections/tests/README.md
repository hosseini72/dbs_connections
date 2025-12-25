# Running Tests with unittest

This project uses Python's built-in `unittest` framework for testing.

## Running Tests

### Run all tests
```bash
# From project root
python -m unittest discover -s tests -p "test_*.py" -v

# Or use the helper script
python tests/unittest_config.py
```

### Run specific test module
```bash
python -m unittest tests.unit.connectors.postgres.test_postgres_config -v
```

### Run specific test class
```bash
python -m unittest tests.unit.connectors.postgres.test_postgres_config.TestPostgresPoolConfig -v
```

### Run specific test method
```bash
python -m unittest tests.unit.connectors.postgres.test_postgres_config.TestPostgresPoolConfig.test_config_creation -v
```

### Run tests with coverage (requires coverage.py)
```bash
# Install coverage if not already installed
pip install coverage

# Run tests with coverage
coverage run -m unittest discover -s tests -p "test_*.py"

# Generate coverage report
coverage report

# Generate HTML coverage report
coverage html
```

## Test Organization

- `tests/unit/` - Unit tests (fast, no external dependencies)
- `tests/unit/connectors/` - Database connector tests
- `tests/unit/integration/` - Integration tests (require external services)

## Test Naming Conventions

- Test files: `test_*.py`
- Test classes: `Test*`
- Test methods: `test_*`

## Example Test Structure

```python
import unittest
from db_connections.scr.all_db_connectors.connectors.postgres import PostgresPoolConfig

class TestPostgresPoolConfig(unittest.TestCase):
    def test_config_creation(self):
        config = PostgresPoolConfig(
            host="localhost",
            database="testdb"
        )
        self.assertEqual(config.host, "localhost")
        self.assertEqual(config.database, "testdb")
```

## Environment Variables

Set these environment variables to configure test behavior:

- `DB_CONNECTIONS_TEST_MODE`: Set to 'unit' or 'integration'
- `DB_CONNECTIONS_VERBOSE`: Set to '1' for verbose output

## Notes

- Unittest is part of Python's standard library (no external dependencies needed)
- For async tests, use `unittest.IsolatedAsyncioTestCase` (Python 3.8+)
- Integration tests may require external services (PostgreSQL, Redis, etc.)

