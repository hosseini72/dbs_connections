# This file is kept for backward compatibility
# Comprehensive tests are in:
# - test_rabbitmq_config.py
# - test_rabbitmq_pool.py
# - test_rabbitmq_pool_async.py
# - test_rabbitmq_health.py

test_rabbitmq_config = {
    "host": "localhost",
    "port": 5672,
    "username": "test_user",
    "password": "test_pass",
    "virtual_host": "test_vhost",
    "ssl": False,
    "ssl_options": None,
    "heartbeat": 60,
    "connection_attempts": 3,
    "retry_delay": 5,
}
