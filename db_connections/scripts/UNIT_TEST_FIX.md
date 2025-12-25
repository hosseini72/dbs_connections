# Unit Test Discovery Fix

## Problem
When running `python -m unittest discover -s tests/unit -p "test_*.py"`, it was discovering integration tests in `tests/unit/integration/` which were all being skipped because PostgreSQL wasn't available.

## Solution
Updated all test commands to exclude the `integration/` directory when running unit tests.

## Updated Commands

### Unit Tests (Excludes Integration)
```bash
# Run connector unit tests
python -m unittest discover -s tests/unit/connectors -p "test_*.py" -v

# Run core unit tests
python -m unittest tests.unit.test_core -p "test_*.py" -v

# Or use the script
.\scripts\run_tests.ps1 unit -Verbose
```

### Integration Tests (Separate)
```bash
# Run integration tests (requires Docker)
python -m unittest discover -s tests/unit/integration -p "test_*.py" -v

# Or use the script
.\scripts\run_tests.ps1 integration -Verbose
```

### All Tests
```bash
# Run everything (unit + integration)
python -m unittest discover -s tests -p "test_*.py" -v

# Or use the script
.\scripts\run_tests.ps1 all -Verbose
```

## Files Updated
- `Makefile` - Updated test commands
- `scripts/run_tests.sh` - Updated unit test discovery
- `scripts/run_tests.ps1` - Updated unit test discovery
- `scripts/TESTING_GUIDE.md` - Updated documentation
- `scripts/quick_start.md` - Updated documentation

## Test Structure
```
tests/
├── unit/
│   ├── connectors/        # Unit tests (no external dependencies)
│   │   ├── postgres/
│   │   ├── redis/
│   │   └── ...
│   ├── integration/       # Integration tests (require Docker)
│   │   └── test_postgres_integration.py
│   └── test_core.py       # Core unit tests
```

Now unit tests and integration tests are properly separated!

