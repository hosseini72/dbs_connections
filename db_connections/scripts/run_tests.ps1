# PowerShell script to run tests
# Usage: .\scripts\run_tests.ps1 [test_type] [-VerboseOutput]

param(
    [Parameter(Position=0)]
    [string]$TestType = "all",
    
    [switch]$VerboseOutput
)

$ErrorActionPreference = "Stop"

# Initialize exit code
$script:ExitCode = 0

# Colors for output
function Write-Info {
    Write-Host "[i] $args" -ForegroundColor Blue
}

function Write-Success {
    Write-Host "[OK] $args" -ForegroundColor Green
}

function Write-ErrorMsg {
    Write-Host "[X] $args" -ForegroundColor Red
}

function Write-Warning {
    Write-Host "[!] $args" -ForegroundColor Yellow
}

# Print banner
Write-Host "========================================" -ForegroundColor Blue
Write-Host "   DB Connections - Test Runner" -ForegroundColor Blue
Write-Host "========================================" -ForegroundColor Blue
Write-Host ""

# Check if Python is available
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    Write-ErrorMsg "Python is not installed!"
    exit 1
}

$VerboseFlag = if ($VerboseOutput) { "-v" } else { "" }

switch ($TestType.ToLower()) {
    "unit" {
        Write-Info "Running unit tests..."
        Write-Info "Running connector tests..."
        python -m unittest discover -s tests/unit/connectors -p "test_*.py" $VerboseFlag
        $script:ExitCode = $LASTEXITCODE
        
        # Run core tests if the file exists and is not empty
        if (Test-Path "tests/unit/test_core.py") {
            $fileContent = Get-Content "tests/unit/test_core.py" -Raw
            if ($fileContent -and $fileContent.Trim().Length -gt 0) {
                Write-Info "Running core tests..."
                python -m unittest tests.unit.test_core $VerboseFlag
                if ($LASTEXITCODE -ne 0) {
                    $script:ExitCode = $LASTEXITCODE
                }
            }
        }
    }
    
    "integration" {
        Write-Info "Running integration tests..."
        Write-Warning "Make sure Docker containers are running!"
        Write-Host ""
        Write-Info "Start containers with:"
        Write-Host "  docker-compose -f tests/unit/integration/docker-compose.yml up -d"
        Write-Host ""
        
        # Check if PostgreSQL container is running
        $postgresRunning = docker ps 2>$null | Select-String "mycompany_test_postgres"
        if ($postgresRunning) {
            Write-Success "PostgreSQL container is running"
        } else {
            Write-ErrorMsg "PostgreSQL container not found!"
            Write-Info "Starting containers..."
            docker-compose -f tests/unit/integration/docker-compose.yml up -d
            Start-Sleep -Seconds 5
        }
        
        python -m unittest discover -s tests/unit/integration -p "test_*.py" $VerboseFlag
        $script:ExitCode = $LASTEXITCODE
    }
    
    "postgres" {
        Write-Info "Running PostgreSQL-specific tests..."
        python -m unittest discover -s tests/unit/connectors/postgres -p "test_*.py" $VerboseFlag
        if ($LASTEXITCODE -ne 0) { $script:ExitCode = $LASTEXITCODE }
        python -m unittest discover -s tests/unit/integration -p "test_postgres*.py" $VerboseFlag
        if ($LASTEXITCODE -ne 0) { $script:ExitCode = $LASTEXITCODE }
    }
    
    "redis" {
        Write-Info "Running Redis-specific tests..."
        python -m unittest discover -s tests/unit/connectors/redis -p "test_*.py" $VerboseFlag
        $script:ExitCode = $LASTEXITCODE
    }
    
    "mongodb" {
        Write-Info "Running MongoDB-specific tests..."
        python -m unittest discover -s tests/unit/connectors/mongodb -p "test_*.py" $VerboseFlag
        $script:ExitCode = $LASTEXITCODE
    }
    
    "clickhouse" {
        Write-Info "Running ClickHouse-specific tests..."
        python -m unittest discover -s tests/unit/connectors/clickhouse -p "test_*.py" $VerboseFlag
        $script:ExitCode = $LASTEXITCODE
    }
    
    "rabbitmq" {
        Write-Info "Running RabbitMQ-specific tests..."
        python -m unittest discover -s tests/unit/connectors/rabbitmq -p "test_*.py" $VerboseFlag
        $script:ExitCode = $LASTEXITCODE
    }
    
    "neo4j" {
        Write-Info "Running Neo4j-specific tests..."
        python -m unittest discover -s tests/unit/connectors/neo4j -p "test_*.py" $VerboseFlag
        $script:ExitCode = $LASTEXITCODE
    }
    
    "config" {
        Write-Info "Running configuration tests..."
        python -m unittest discover -s tests/unit/connectors -p "test_*_config.py" $VerboseFlag
        $script:ExitCode = $LASTEXITCODE
    }
    
    "health" {
        Write-Info "Running health check tests..."
        python -m unittest discover -s tests/unit/connectors -p "test_*_health.py" $VerboseFlag
        $script:ExitCode = $LASTEXITCODE
    }
    
    "coverage" {
        Write-Info "Running tests with coverage report..."
        if (-not (Get-Command coverage -ErrorAction SilentlyContinue)) {
            Write-Warning "coverage.py not found. Installing..."
            pip install coverage
        }
        coverage run -m unittest discover -s tests -p "test_*.py"
        $script:ExitCode = $LASTEXITCODE
        coverage report
        coverage html
        Write-Success "Coverage report generated:"
        Write-Info "  HTML: htmlcov/index.html"
        Write-Info "  Terminal: See above"
    }
    
    "all" {
        Write-Info "Running all tests..."
        python -m unittest discover -s tests -p "test_*.py" $VerboseFlag
        $script:ExitCode = $LASTEXITCODE
    }
    
    "help" {
        Write-Host "Usage: .\scripts\run_tests.ps1 [test_type] [-Verbose]"
        Write-Host ""
        Write-Host "Test types:"
        Write-Host "  unit          - Run unit tests only (fast)"
        Write-Host "  integration   - Run integration tests (requires Docker)"
        Write-Host "  postgres      - Run PostgreSQL-specific tests"
        Write-Host "  redis         - Run Redis-specific tests"
        Write-Host "  mongodb       - Run MongoDB-specific tests"
        Write-Host "  clickhouse    - Run ClickHouse-specific tests"
        Write-Host "  rabbitmq      - Run RabbitMQ-specific tests"
        Write-Host "  neo4j         - Run Neo4j-specific tests"
        Write-Host "  config        - Run configuration tests"
        Write-Host "  health        - Run health check tests"
        Write-Host "  coverage      - Run tests with coverage report"
        Write-Host "  all           - Run all tests (default)"
        Write-Host "  help           - Show this help message"
        Write-Host ""
        Write-Host "Options:"
        Write-Host "  -VerboseOutput      - Verbose output"
        Write-Host ""
        Write-Host "Examples:"
        Write-Host "  .\scripts\run_tests.ps1 unit"
        Write-Host "  .\scripts\run_tests.ps1 integration -VerboseOutput"
        Write-Host "  .\scripts\run_tests.ps1 coverage"
        Write-Host ""
        exit 0
    }
    
    default {
        Write-ErrorMsg "Unknown test type: $TestType"
        Write-Host ""
        Write-Host "Run '.\scripts\run_tests.ps1 help' for usage information"
        exit 1
    }
}

# Get exit code (use script variable if set, otherwise LASTEXITCODE)
if ($null -ne $script:ExitCode) {
    $ExitCode = $script:ExitCode
} else {
    $ExitCode = if ($LASTEXITCODE) { $LASTEXITCODE } else { 0 }
}

Write-Host ""
if ($ExitCode -eq 0) {
    Write-Success "All tests passed!"
} else {
    Write-ErrorMsg "Some tests failed!"
}

exit $ExitCode

