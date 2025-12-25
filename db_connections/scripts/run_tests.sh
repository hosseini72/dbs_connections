#!/bin/bash
# scripts/run_tests.sh
# Run different types of tests for db_connections using unittest

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

# Check if Python is available
if ! command -v python &> /dev/null; then
    print_error "Python is not installed!"
    exit 1
fi

# Parse command line arguments
TEST_TYPE="${1:-all}"
VERBOSE="${2:-}"

# Add verbose flag if provided
if [ "$VERBOSE" = "-v" ] || [ "$VERBOSE" = "-vv" ]; then
    VERBOSE_FLAG="-v"
else
    VERBOSE_FLAG=""
fi

case $TEST_TYPE in
    unit)
        print_info "Running unit tests..."
        print_info "Running connector tests..."
        python -m unittest discover -s tests/unit/connectors -p "test_*.py" $VERBOSE_FLAG
        
        # Run core tests if the file exists
        if [ -f "tests/unit/test_core.py" ]; then
            print_info "Running core tests..."
            python -m unittest tests.unit.test_core $VERBOSE_FLAG
        fi
        ;;
    
    integration)
        print_info "Running integration tests..."
        print_warning "Make sure Docker containers are running!"
        echo ""
        print_info "Start containers with:"
        echo "  docker-compose -f tests/unit/integration/docker-compose.yml up -d"
        echo ""
        
        # Check if PostgreSQL container is running
        if docker ps 2>/dev/null | grep -q mycompany_test_postgres; then
            print_status "PostgreSQL container is running"
        else
            print_error "PostgreSQL container not found!"
            print_info "Starting containers..."
            docker-compose -f tests/unit/integration/docker-compose.yml up -d
            sleep 5
        fi
        
        python -m unittest discover -s tests/unit/integration -p "test_*.py" $VERBOSE_FLAG
        ;;
    
    postgres)
        print_info "Running PostgreSQL-specific tests..."
        python -m unittest discover -s tests/unit/connectors/postgres -p "test_*.py" $VERBOSE_FLAG
        python -m unittest discover -s tests/unit/integration -p "test_postgres*.py" $VERBOSE_FLAG
        ;;
    
    redis)
        print_info "Running Redis-specific tests..."
        python -m unittest discover -s tests/unit/connectors/redis -p "test_*.py" $VERBOSE_FLAG
        ;;
    
    mongodb)
        print_info "Running MongoDB-specific tests..."
        python -m unittest discover -s tests/unit/connectors/mongodb -p "test_*.py" $VERBOSE_FLAG
        ;;
    
    clickhouse)
        print_info "Running ClickHouse-specific tests..."
        python -m unittest discover -s tests/unit/connectors/clickhouse -p "test_*.py" $VERBOSE_FLAG
        ;;
    
    rabbitmq)
        print_info "Running RabbitMQ-specific tests..."
        python -m unittest discover -s tests/unit/connectors/rabbitmq -p "test_*.py" $VERBOSE_FLAG
        ;;
    
    neo4j)
        print_info "Running Neo4j-specific tests..."
        python -m unittest discover -s tests/unit/connectors/neo4j -p "test_*.py" $VERBOSE_FLAG
        ;;
    
    config)
        print_info "Running configuration tests..."
        python -m unittest discover -s tests/unit/connectors -p "test_*_config.py" $VERBOSE_FLAG
        ;;
    
    health)
        print_info "Running health check tests..."
        python -m unittest discover -s tests/unit/connectors -p "test_*_health.py" $VERBOSE_FLAG
        ;;
    
    coverage)
        print_info "Running tests with coverage report..."
        if ! command -v coverage &> /dev/null; then
            print_warning "coverage.py not found. Installing..."
            pip install coverage
        fi
        coverage run -m unittest discover -s tests -p "test_*.py"
        coverage report
        coverage html
        print_status "Coverage report generated:"
        print_info "  HTML: htmlcov/index.html"
        print_info "  Terminal: See above"
        ;;
    
    all)
        print_info "Running all tests..."
        python -m unittest discover -s tests -p "test_*.py" $VERBOSE_FLAG
        ;;
    
    help|--help|-h)
        echo "Usage: $0 [test_type] [verbose]"
        echo ""
        echo "Test types:"
        echo "  unit          - Run unit tests only (fast)"
        echo "  integration   - Run integration tests (requires Docker)"
        echo "  postgres      - Run PostgreSQL-specific tests"
        echo "  redis         - Run Redis-specific tests"
        echo "  mongodb       - Run MongoDB-specific tests"
        echo "  clickhouse    - Run ClickHouse-specific tests"
        echo "  rabbitmq      - Run RabbitMQ-specific tests"
        echo "  neo4j         - Run Neo4j-specific tests"
        echo "  config        - Run configuration tests"
        echo "  health        - Run health check tests"
        echo "  coverage      - Run tests with coverage report"
        echo "  all           - Run all tests (default)"
        echo "  help          - Show this help message"
        echo ""
        echo "Verbose options:"
        echo "  -v            - Verbose output"
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
