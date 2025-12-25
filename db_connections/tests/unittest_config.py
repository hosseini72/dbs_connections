"""
Unittest configuration and helper utilities.

This module provides configuration and utilities for running tests with unittest.
"""

import os
import sys
import unittest
from pathlib import Path

# Add the project root to the Python path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Test discovery configuration
TEST_PATTERNS = {
    'test_files': 'test_*.py',
    'test_classes': 'Test*',
    'test_methods': 'test_*',
}

# Test paths
TEST_PATHS = [
    'tests/unit',
    'tests/unit/connectors',
    'tests/unit/integration',
]

# Coverage configuration (if using coverage.py)
COVERAGE_CONFIG = {
    'source': ['db_connections'],
    'omit': [
        '*/tests/*',
        '*/test_*.py',
        '*/__pycache__/*',
        '*/site-packages/*',
        '*/.venv/*',
    ],
}

# Environment variables for test configuration
TEST_ENV_VARS = {
    'DB_CONNECTIONS_TEST_MODE': 'unit',  # 'unit' or 'integration'
    'DB_CONNECTIONS_VERBOSE': '1',  # Set to '1' for verbose output
}


def configure_test_environment():
    """Configure environment variables for testing."""
    for key, value in TEST_ENV_VARS.items():
        if key not in os.environ:
            os.environ[key] = value


def get_test_loader():
    """Get a configured unittest test loader."""
    loader = unittest.TestLoader()
    loader.testMethodPrefix = 'test_'
    return loader


def discover_tests(start_dir=None, pattern='test_*.py'):
    """Discover and return all tests."""
    if start_dir is None:
        start_dir = PROJECT_ROOT / 'tests'
    
    loader = get_test_loader()
    suite = loader.discover(
        str(start_dir),
        pattern=pattern,
        top_level_dir=str(PROJECT_ROOT)
    )
    return suite


if __name__ == '__main__':
    # Configure environment
    configure_test_environment()
    
    # Discover and run tests
    suite = discover_tests()
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1)

