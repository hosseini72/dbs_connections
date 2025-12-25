# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2025-12-19

### Added
- Initial release of db_connections library
- Support for PostgreSQL with both sync (psycopg2) and async (asyncpg) drivers
- Support for Redis with both sync and async drivers
- Support for MongoDB with both sync (pymongo) and async (motor) drivers
- Support for ClickHouse with sync and async drivers
- Support for RabbitMQ with both sync (pika) and async (aio-pika) drivers
- Support for Neo4j with sync and async drivers
- Connection pooling for all database connectors
- Health check functionality for all connectors
- Metrics collection for connection pools
- Configuration via environment variables
- Configuration via connection strings/URLs
- SSL/TLS support for all connectors
- Comprehensive setup guides for each database connector
- Type hints throughout the codebase
- Framework integration examples (FastAPI, Flask, Django)

### Features
- **Connection Pooling**: Configurable min/max pool sizes with overflow handling
- **Health Monitoring**: Built-in health checks for pools and database servers
- **Metrics**: Track active/idle connections, wait times, and pool utilization
- **Reliability**: Automatic reconnection, connection validation, and retry logic
- **Flexibility**: Support for both synchronous and asynchronous operations
- **Configuration**: Multiple configuration methods (direct, env vars, connection strings)

### Documentation
- Complete README with installation and usage examples
- Detailed setup guides for each database connector
- API reference documentation
- Best practices guide
- Configuration guide
- Migration guide

### Database Connectors

#### PostgreSQL
- Sync: `PostgresConnectionPool` using psycopg2
- Async: `AsyncPostgresConnectionPool` using asyncpg
- Features: Transaction support, connection string (DSN) support, SSL/TLS

#### Redis
- Sync: `RedisSyncConnectionPool` using redis
- Async: `RedisAsyncConnectionPool` using redis[hiredis]
- Features: Pipeline support, connection URL support, SSL/TLS

#### MongoDB
- Sync: `MongoSyncConnectionPool` using pymongo
- Async: `MongoAsyncConnectionPool` using motor
- Features: Replica set support, read preferences, write concerns, SSL/TLS

#### ClickHouse
- Sync: `ClickHouseSyncConnectionPool` using clickhouse-connect
- Async: `ClickHouseAsyncConnectionPool` using clickhouse-connect
- Features: Native and HTTP protocol support, cluster support, compression

#### RabbitMQ
- Sync: `RabbitMQSyncConnectionPool` using pika
- Async: `RabbitMQAsyncConnectionPool` using aio-pika
- Features: All exchange types (direct, fanout, topic, headers), queue management, message acknowledgments

#### Neo4j
- Sync: `Neo4jSyncConnectionPool` using neo4j
- Async: `Neo4jAsyncConnectionPool` using neo4j
- Features: Bolt and HTTP protocol support, Neo4j routing (cluster support), SSL/TLS

### Installation
```bash
# Basic installation
pip install db_connections

# With specific database support
pip install db_connections[postgres]
pip install db_connections[redis]
pip install db_connections[mongodb]
pip install db_connections[clickhouse]
pip install db_connections[rabbitmq]
pip install db_connections[neo4j]

# With all databases
pip install db_connections[all]
```

### Requirements
- Python 3.8 or higher
- Individual database drivers (installed via optional dependencies)

---

## [Unreleased]

### Planned
- Additional database connectors
- Enhanced monitoring and observability
- Performance optimizations
- Extended framework integrations

[1.0.0]: https://github.com/hosseini72/dbs_connections/releases/tag/v1.0.0

