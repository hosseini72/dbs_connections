#!/bin/bash
# scripts/teardown_test_env.sh
# Teardown test environment and cleanup

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}╔════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Tearing Down Test Environment       ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════╝${NC}"
echo ""

# Stop containers
echo -e "${GREEN}[1/3]${NC} Stopping Docker containers..."
docker-compose -f tests/unit/integration/docker-compose.yml down || true

# Ask about removing volumes
echo ""
echo -e "${YELLOW}[?]${NC} Do you want to remove test data volumes?"
echo "    This will delete all test data permanently."
read -p "    Remove volumes? (y/N): " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${GREEN}[2/3]${NC} Removing volumes..."
    docker-compose -f tests/unit/integration/docker-compose.yml down -v || true
    echo -e "${GREEN}✓${NC} Volumes removed"
else
    echo -e "${YELLOW}[2/3]${NC} Keeping volumes (skipped)"
fi

# Clean test artifacts
echo -e "${GREEN}[3/3]${NC} Cleaning test artifacts..."
rm -rf htmlcov || true
rm -rf .coverage || true
rm -rf coverage.xml || true
find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
find . -type f -name "*.pyc" -delete 2>/dev/null || true
find . -type f -name "*.pyo" -delete 2>/dev/null || true

echo ""
echo -e "${GREEN}╔════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║   Cleanup Complete! ✓                  ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════╝${NC}"
echo ""
echo "Test environment has been torn down."
echo ""
echo "To setup again, run:"
echo "  ./scripts/setup_test_env.sh"
