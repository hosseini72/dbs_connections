#!/bin/bash
# scripts/setup_test_env.sh

set -e

GREEN='\033[0;32m'
NC='\033[0m'

echo -e "${GREEN}Setting up test environment...${NC}"

# Start Docker containers
echo "Starting test containers..."
docker-compose -f tests/unit/integration/docker-compose.yml up -d

# Wait for services to be healthy
echo "Waiting for services to be ready..."
sleep 10

# Check PostgreSQL
echo "Checking PostgreSQL..."
docker exec mycompany_test_postgres pg_isready -U test_user -d test_db || true

# Check Redis
echo "Checking Redis..."
docker exec mycompany_test_redis redis-cli ping || true

# Install test dependencies (unittest is built-in, but we may need database drivers)
echo "Installing test dependencies..."
pip install -e ".[postgres,redis,mongodb,clickhouse,rabbitmq,neo4j]" || true

echo -e "${GREEN}Test environment ready!${NC}"
echo ""
echo "Run tests with:"
echo "  ./scripts/run_tests.sh unit"
echo "  ./scripts/run_tests.sh integration"
echo "  ./scripts/run_tests.sh all"
echo ""
echo "Or use unittest directly:"
echo "  python -m unittest discover -s tests -p \"test_*.py\" -v"
