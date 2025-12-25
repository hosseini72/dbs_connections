#!/bin/bash
# scripts/run_tests.sh
# Run different types of tests for db_connections

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print banner
echo -e "${BLUE}╔════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   DB Connections - Test Runner          ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════╝${NC}"
echo ""

# Function to print colored output
print_status() {
    echo -e "${GREEN}[✓]${NC} $1"
}

print_error() {
    echo -e "${RED}[✗]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[!]${NC} $1"
}

print_info() {
    echo -e "${BLUE}[i]${NC} $1"
}

# Check if pytest is installed
if ! command -v pytest &> /dev/null; then
    print_error "pytest is not installed!"
    echo "Install with: pip install -e \".[test]\""
    exit 1
fi

# Parse command line arguments
TEST_TYPE="${1:-all}"
VERBOSE="${2:-}"

# Add verbose flag if provided
if [ "$VERBOSE" = "-v" ] || [ "$VERBOSE" = "-vv" ]; then
    VERBOSE_FLAG="$VERBOSE"
else
    VERBOSE_FLAG=""
fi

case $TEST_TYPE in
    unit)
        print_info "Running unit tests..."
        pytest tests/unit -m unit $VERBOSE_FLAG
        ;;
    
    integration)
        print_info "Running integration tests..."
        print_warning "Make sure Docker containers are running!"
        echo ""
        print_info "Start containers with:"
        echo "  docker-compose -f tests/integration/docker-compose.yml up -d"
        echo ""
        
        # Check if PostgreSQL container is running
        if docker ps 2>/dev/null | grep -q mycompany_test_postgres; then
            print_status "PostgreSQL container is running"
        else
            print_error "PostgreSQL container not found!"
            print_info "Starting containers..."
            docker-compose -f tests/integration/docker-compose.yml up -d
            sleep 5
        fi
        
        pytest tests/integration -m integration $VERBOSE_FLAG
        ;;
    
    postgres)
        print_info "Running PostgreSQL-specific tests..."
        pytest tests/unit/connectors/test_postgres*.py tests/integration/test_postgres*.py -m postgres $VERBOSE_FLAG
        ;;
    
    config)
        print_info "Running configuration tests..."
        pytest tests/unit/connectors/test_postgres_config.py $VERBOSE_FLAG
        ;;
    
    health)
        print_info "Running health check tests..."
        pytest tests/unit/connectors/test_postgres_health.py $VERBOSE_FLAG
        ;;
    
    coverage)
        print_info "Running tests with coverage report..."
        pytest tests/ \
            --cov=scr/all_db_connectors \
            --cov-report=html \
            --cov-report=term-missing \
            --cov-report=xml \
            $VERBOSE_FLAG
        print_status "Coverage report generated:"
        print_info "  HTML: htmlcov/index.html"
        print_info "  XML:  coverage.xml"
        ;;
    
    fast)
        print_info "Running fast tests only (excluding slow tests)..."
        pytest tests/ -m "not slow" $VERBOSE_FLAG
        ;;
    
    watch)
        print_info "Running tests in watch mode..."
        print_warning "This requires pytest-watch: pip install pytest-watch"
        ptw tests/ -- -m unit $VERBOSE_FLAG
        ;;
    
    all)
        print_info "Running all tests..."
        pytest tests/ $VERBOSE_FLAG
        ;;
    
    list)
        print_info "Available test markers:"
        pytest --markers
        ;;
    
    help|--help|-h)
        echo "Usage: $0 [test_type] [verbose]"
        echo ""
        echo "Test types:"
        echo "  unit          - Run unit tests only (fast)"
        echo "  integration   - Run integration tests (requires Docker)"
        echo "  postgres      - Run PostgreSQL-specific tests"
        echo "  config        - Run configuration tests"
        echo "  health        - Run health check tests"
        echo "  coverage      - Run tests with coverage report"
        echo "  fast          - Run fast tests only (exclude slow)"
        echo "  watch         - Run tests in watch mode"
        echo "  all           - Run all tests (default)"
        echo "  list          - List all available test markers"
        echo "  help          - Show this help message"
        echo ""
        echo "Verbose options:"
        echo "  -v            - Verbose output"
        echo "  -vv           - Very verbose output"
        echo ""
        echo "Examples:"
        echo "  $0 unit              # Run unit tests"
        echo "  $0 integration -v    # Run integration tests with verbose output"
        echo "  $0 coverage          # Generate coverage report"
        echo ""
        exit 0
        ;;
    
    *)
        print_error "Unknown test type: $TEST_TYPE"
        echo ""
        echo "Run '$0 help' for usage information"
        exit 1
        ;;
esac

EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    print_status "All tests passed! ✓"
else
    print_error "Some tests failed! ✗"
fi

exit $EXIT_CODE