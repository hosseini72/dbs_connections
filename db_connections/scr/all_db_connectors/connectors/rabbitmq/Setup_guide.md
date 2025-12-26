# RabbitMQ Connector - Installation & Setup Guide

## Table of Contents

1. [Installation](#installation)
2. [Dependencies](#dependencies)
3. [Quick Start](#quick-start)
4. [Configuration Options](#configuration-options)
5. [Exchange Types](#exchange-types)
6. [Queue Types](#queue-types)
7. [Messaging Patterns](#messaging-patterns)
8. [Advanced Usage](#advanced-usage)
9. [Health Checks](#health-checks)
10. [Best Practices](#best-practices)
11. [Troubleshooting](#troubleshooting)

---

## Installation

### Option 1: Install with Sync Support (pika)

```bash
pip install db_connections[rabbitmq-sync]
```

### Option 2: Install with Async Support (aio_pika)

```bash
pip install db_connections[rabbitmq-async]
```

### Option 3: Install with Both Sync and Async

```bash
pip install db_connections[rabbitmq]
```

---

## Quick Start

### Synchronous Usage

```python
from db_connections.scr.all_db_connectors.connectors.rabbitmq import (
    RabbitMQSyncConnectionPool,
    RabbitMQPoolConfig,
)

# Configure pool
config = RabbitMQPoolConfig(
    host="localhost",
    port=5672,
    username="guest",
    password="guest",
    virtual_host="/",
    min_connections=2,
    max_connections=10,
)

# Use pool with context manager
with RabbitMQSyncConnectionPool(config) as pool:
    with pool.get_connection() as conn:
        channel = conn.channel()
        
        # Declare a queue
        channel.queue_declare(queue='my_queue', durable=True)
        
        # Publish a message
        channel.basic_publish(
            exchange='',
            routing_key='my_queue',
            body='Hello, RabbitMQ!'
        )
        
        channel.close()
```

### Asynchronous Usage

```python
from db_connections.scr.all_db_connectors.connectors.rabbitmq import (
    RabbitMQAsyncConnectionPool,
    RabbitMQPoolConfig,
)

config = RabbitMQPoolConfig(
    host="localhost",
    port=5672,
    username="guest",
    password="guest",
)

async with RabbitMQAsyncConnectionPool(config) as pool:
    async with pool.get_connection() as conn:
        channel = await conn.channel()
        
        # Declare queue
        queue = await channel.declare_queue('my_queue', durable=True)
        
        # Publish message
        await channel.default_exchange.publish(
            aio_pika.Message(b'Hello, RabbitMQ!'),
            routing_key='my_queue'
        )
        
        await channel.close()
```

---

## Configuration Options

### Basic Configuration

```python
config = RabbitMQPoolConfig(
    # Connection details
    host="localhost",
    port=5672,
    username="guest",
    password="guest",
    virtual_host="/",
    
    # Pool settings
    min_connections=2,
    max_connections=10,
    
    # Connection behavior
    timeout=30,
    connection_timeout=30,
    socket_timeout=10,
    heartbeat=600,
    connection_attempts=3,
    retry_delay=2.0,
)
```

### Connection URL Configuration

```python
# Using connection URL (overrides individual parameters)
config = RabbitMQPoolConfig(
    connection_url="amqp://username:password@localhost:5672/vhost",
)

# Or using from_url class method
config = RabbitMQPoolConfig.from_url(
    "amqp://username:password@localhost:5672/vhost",
    min_connections=2,
    max_connections=10,
)
```

### SSL/TLS Configuration

```python
config = RabbitMQPoolConfig(
    host="rabbitmq.example.com",
    port=5671,  # AMQPS port
    ssl=True,
    ssl_options={
        'ca_certs': '/path/to/ca_certificate.pem',
        'certfile': '/path/to/client_certificate.pem',
        'keyfile': '/path/to/client_key.pem',
        'cert_reqs': ssl.CERT_REQUIRED,
        'ssl_version': ssl.PROTOCOL_TLSv1_2,
    }
)
```

### Environment Variables Configuration

```python
# Load from environment variables
config = RabbitMQPoolConfig.from_env(prefix="RABBITMQ_")
```

Set environment variables:
- `RABBITMQ_HOST=localhost`
- `RABBITMQ_PORT=5672`
- `RABBITMQ_USERNAME=guest`
- `RABBITMQ_PASSWORD=guest`
- `RABBITMQ_VIRTUAL_HOST=/`
- `RABBITMQ_CONNECTION_URL=amqp://localhost:5672/`

---

## Exchange Types

RabbitMQ supports four types of exchanges. Each has different routing behavior:

### 1. Direct Exchange

**Use Case**: Point-to-point routing, exact routing key matching

Routes messages to queues based on exact routing key match.

```python
channel.exchange_declare(
    exchange='direct_logs',
    exchange_type='direct',
    durable=True
)

# Bind queues with routing keys
channel.queue_bind(
    exchange='direct_logs',
    queue='info_queue',
    routing_key='info'
)

channel.queue_bind(
    exchange='direct_logs',
    queue='error_queue',
    routing_key='error'
)

# Publish with routing key
channel.basic_publish(
    exchange='direct_logs',
    routing_key='info',  # Goes to info_queue
    body='Info message'
)
```

**When to use**: Logging systems, priority-based routing, simple routing scenarios.

---

### 2. Fanout Exchange

**Use Case**: Broadcast to all bound queues (publish/subscribe pattern)

Routes messages to ALL bound queues, ignoring routing key.

```python
channel.exchange_declare(
    exchange='notifications',
    exchange_type='fanout',
    durable=True
)

# Bind multiple queues (all receive the message)
channel.queue_bind(exchange='notifications', queue='email_queue')
channel.queue_bind(exchange='notifications', queue='sms_queue')
channel.queue_bind(exchange='notifications', queue='push_queue')

# Publish (routing_key is ignored)
channel.basic_publish(
    exchange='notifications',
    routing_key='',  # Ignored for fanout
    body='New user registered!'  # Goes to ALL queues
)
```

**When to use**: News feeds, notification systems, cache invalidation, pub/sub scenarios.

---

### 3. Topic Exchange

**Use Case**: Pattern-based routing using wildcards

Routes messages based on routing key patterns with wildcards:
- `*` (star) matches exactly one word
- `#` (hash) matches zero or more words

```python
channel.exchange_declare(
    exchange='topic_logs',
    exchange_type='topic',
    durable=True
)

# Bind with patterns
channel.queue_bind(
    exchange='topic_logs',
    queue='all_logs',
    routing_key='#'  # Receives all messages
)

channel.queue_bind(
    exchange='topic_logs',
    queue='error_logs',
    routing_key='*.error'  # Any service's error logs
)

channel.queue_bind(
    exchange='topic_logs',
    queue='user_events',
    routing_key='user.*'  # All user-related events
)

# Publish with routing keys
channel.basic_publish(
    exchange='topic_logs',
    routing_key='order.error',  # Matches *.error pattern
    body='Order processing error'
)

channel.basic_publish(
    exchange='topic_logs',
    routing_key='user.created',  # Matches user.* pattern
    body='User created'
)
```

**Routing Key Examples**:
- `user.created` → matches `user.*`, `#`
- `order.error` → matches `*.error`, `#`
- `payment.success` → matches `#`
- `user.error` → matches `user.*`, `*.error`, `#`

**When to use**: Logging systems, event routing, multi-level routing scenarios.

---

### 4. Headers Exchange

**Use Case**: Route based on message headers (not routing keys)

Routes messages based on header attributes instead of routing keys.

```python
channel.exchange_declare(
    exchange='headers_exchange',
    exchange_type='headers',
    durable=True
)

# Bind with header requirements
# x-match: 'all' = all headers must match, 'any' = any header matches
channel.queue_bind(
    exchange='headers_exchange',
    queue='urgent_queue',
    arguments={
        'x-match': 'all',  # All headers must match
        'priority': 'high',
        'type': 'urgent'
    }
)

channel.queue_bind(
    exchange='headers_exchange',
    queue='notification_queue',
    arguments={
        'x-match': 'any',  # Any header matches
        'priority': 'high',
        'priority': 'medium'
    }
)

# Publish with headers
import pika
channel.basic_publish(
    exchange='headers_exchange',
    routing_key='',  # Ignored for headers exchange
    body='Urgent message',
    properties=pika.BasicProperties(
        headers={
            'priority': 'high',
            'type': 'urgent'
        }
    )
)
```

**When to use**: Complex routing logic, metadata-based routing, when routing keys aren't sufficient.

---

### Default Exchange

The default exchange (empty string `''`) is a direct exchange that routes messages to queues with the same name as the routing key.

```python
# This is equivalent to:
# 1. Declaring a direct exchange with name = ''
# 2. Binding queue 'my_queue' with routing_key 'my_queue'
channel.basic_publish(
    exchange='',  # Default exchange
    routing_key='my_queue',
    body='Message'
)
```

---

## Queue Types

### 1. Durable Queue

Survives RabbitMQ server restarts. Messages must also be marked as persistent.

```python
channel.queue_declare(
    queue='durable_queue',
    durable=True,  # Queue survives server restart
    exclusive=False,
    auto_delete=False
)

# Publish persistent message
channel.basic_publish(
    exchange='',
    routing_key='durable_queue',
    body='Persistent message',
    properties=pika.BasicProperties(
        delivery_mode=2,  # 2 = persistent
    )
)
```

**When to use**: Critical messages that must survive server restarts.

---

### 2. Exclusive Queue

Only accessible by the connection that declared it. Automatically deleted when connection closes.

```python
channel.queue_declare(
    queue='exclusive_queue',
    durable=False,
    exclusive=True,  # Only accessible by this connection
    auto_delete=True
)
```

**When to use**: Temporary queues, RPC reply queues, connection-specific queues.

---

### 3. Auto-Delete Queue

Automatically deleted when the last consumer unsubscribes.

```python
channel.queue_declare(
    queue='temp_queue',
    durable=False,
    exclusive=False,
    auto_delete=True  # Deleted when last consumer unsubscribes
)
```

**When to use**: Temporary processing queues, dynamic consumer scenarios.

---

### 4. Queue with TTL (Time To Live)

Messages expire after a specified time.

```python
# Queue-level TTL (all messages expire after 60 seconds)
channel.queue_declare(
    queue='ttl_queue',
    durable=True,
    arguments={'x-message-ttl': 60000}  # Milliseconds
)

# Message-level TTL (this specific message expires)
channel.basic_publish(
    exchange='',
    routing_key='ttl_queue',
    body='Message with expiration',
    properties=pika.BasicProperties(
        expiration='30000',  # 30 seconds (string format)
    )
)
```

**When to use**: Time-sensitive data, cache expiration, temporary messages.

---

### 5. Queue with Max Length

Limits the number of messages in the queue.

```python
channel.queue_declare(
    queue='limited_queue',
    durable=True,
    arguments={
        'x-max-length': 100,  # Max 100 messages
        # Optional: behavior when max length reached
        'x-overflow': 'drop-head'  # or 'reject-publish'
    }
)
```

**Options for `x-overflow`**:
- `drop-head`: Remove oldest message when limit reached
- `reject-publish`: Reject new messages when limit reached

**When to use**: Rate limiting, buffer management, preventing queue overflow.

---

### 6. Dead Letter Queue (DLQ)

Routes failed/rejected messages to a separate queue.

```python
# Declare dead letter queue
channel.queue_declare(queue='dlq', durable=True)

# Declare main queue with DLQ configuration
channel.queue_declare(
    queue='main_queue',
    durable=True,
    arguments={
        'x-dead-letter-exchange': '',  # Use default exchange
        'x-dead-letter-routing-key': 'dlq'  # Route to DLQ
    }
)

# Messages that are rejected or expire will go to DLQ
```

**When to use**: Error handling, message auditing, retry mechanisms.

---

### 7. Priority Queue

Messages with higher priority are processed first.

```python
channel.queue_declare(
    queue='priority_queue',
    durable=True,
    arguments={'x-max-priority': 10}  # 0-255, but typically 0-10
)

# Publish message with priority
channel.basic_publish(
    exchange='',
    routing_key='priority_queue',
    body='High priority message',
    properties=pika.BasicProperties(
        priority=10,  # Higher priority (0-255)
    )
)
```

**When to use**: Priority-based processing, urgent message handling.

---

## Messaging Patterns

### 1. Work Queue (Task Queue)

Distribute time-consuming tasks among multiple workers.

```python
# Producer
channel.queue_declare(queue='task_queue', durable=True)

message = 'Task message'
channel.basic_publish(
    exchange='',
    routing_key='task_queue',
    body=message,
    properties=pika.BasicProperties(
        delivery_mode=2,  # Make message persistent
    )
)

# Consumer (Worker)
def callback(ch, method, properties, body):
    print(f"Processing: {body.decode()}")
    # Simulate work
    time.sleep(2)
    ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge completion

channel.basic_qos(prefetch_count=1)  # Fair dispatch (don't dispatch until ack)
channel.basic_consume(
    queue='task_queue',
    on_message_callback=callback,
    auto_ack=False  # Manual acknowledgment
)
```

**Key Points**:
- Use `prefetch_count=1` for fair dispatch
- Use manual acknowledgment (`auto_ack=False`)
- Make messages persistent for durability

---

### 2. Publish/Subscribe Pattern

Broadcast messages to multiple consumers.

```python
# Use fanout exchange
channel.exchange_declare(
    exchange='logs',
    exchange_type='fanout'
)

# Each consumer gets its own temporary queue
result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

channel.queue_bind(exchange='logs', queue=queue_name)

# Publish
channel.basic_publish(
    exchange='logs',
    routing_key='',
    body='Log message'
)
```

---

### 3. Routing Pattern

Route messages selectively based on routing keys (using direct exchange).

See [Direct Exchange](#1-direct-exchange) example above.

---

### 4. Topics Pattern

Route messages based on multiple criteria (using topic exchange).

See [Topic Exchange](#3-topic-exchange) example above.

---

### 5. RPC Pattern (Request-Reply)

Remote procedure call pattern with request/response.

```python
# Server (RPC Worker)
channel.queue_declare(queue='rpc_queue')

def on_request(ch, method, properties, body):
    n = int(body)
    print(f"Calculating fibonacci({n})")
    response = str(fibonacci(n))
    
    ch.basic_publish(
        exchange='',
        routing_key=properties.reply_to,
        properties=pika.BasicProperties(
            correlation_id=properties.correlation_id,
        ),
        body=str(response)
    )
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='rpc_queue', on_message_callback=on_request)

# Client (RPC Caller)
result = channel.queue_declare(queue='', exclusive=True)
callback_queue = result.method.queue

correlation_id = str(uuid.uuid4())
response = None

def on_response(ch, method, properties, body):
    if properties.correlation_id == correlation_id:
        response = body.decode()
        ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue=callback_queue, on_message_callback=on_response)

channel.basic_publish(
    exchange='',
    routing_key='rpc_queue',
    properties=pika.BasicProperties(
        reply_to=callback_queue,
        correlation_id=correlation_id,
    ),
    body='30'
)
```

---

## Advanced Usage

### Connection Pooling

```python
config = RabbitMQPoolConfig(
    host="localhost",
    port=5672,
    min_connections=2,  # Minimum connections in pool
    max_connections=10,  # Maximum connections in pool
)

with RabbitMQSyncConnectionPool(config) as pool:
    # Pool manages connections automatically
    with pool.get_connection() as conn:
        channel = conn.channel()
        # Use channel...
        channel.close()
    
    # Connection is returned to pool automatically
```

### Pool Status and Metrics

```python
with RabbitMQSyncConnectionPool(config) as pool:
    # Get pool status
    status = pool.pool_status()
    print(f"Total connections: {status['total_connections']}")
    print(f"Active connections: {status['active_connections']}")
    print(f"Idle connections: {status['idle_connections']}")
    
    # Get metrics
    metrics = pool.get_metrics()
    print(f"Total requests: {metrics.total_requests}")
    print(f"Average response time: {metrics.avg_response_time_ms}ms")
```

### Message Acknowledgments

**Automatic Acknowledgment** (simple but less reliable):
```python
channel.basic_consume(
    queue='my_queue',
    on_message_callback=callback,
    auto_ack=True  # Automatically acknowledge
)
```

**Manual Acknowledgment** (recommended):
```python
def callback(ch, method, properties, body):
    try:
        # Process message
        process_message(body)
        ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge success
    except Exception as e:
        # Reject and requeue on error
        ch.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue=True  # Put message back in queue
        )
        # Or reject without requeue
        # ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

channel.basic_consume(
    queue='my_queue',
    on_message_callback=callback,
    auto_ack=False  # Manual acknowledgment
)
```

### Prefetch Count (Fair Dispatch)

```python
# Limit unacknowledged messages per consumer
channel.basic_qos(prefetch_count=1)  # Only 1 unacked message at a time

# This ensures fair dispatch - messages are distributed evenly
# among workers even if some process faster than others
```

---

## Health Checks

```python
from db_connections.scr.all_db_connectors.connectors.rabbitmq import (
    RabbitMQHealthChecker,
    RabbitMQPoolConfig,
)

config = RabbitMQPoolConfig(host="localhost", port=5672)
health_checker = RabbitMQHealthChecker(config)

with RabbitMQSyncConnectionPool(config) as pool:
    with pool.get_connection() as conn:
        # Check connection health
        health_status = health_checker.check_health(conn)
        print(f"Health State: {health_status.state}")
        print(f"Response Time: {health_status.response_time_ms}ms")
        print(f"Message: {health_status.message}")
        
        # Check queue status
        channel = conn.channel()
        queue_status = health_checker.check_queue_status(
            channel, 'my_queue'
        )
        if queue_status:
            print(f"Queue: {queue_status['queue']}")
            print(f"Messages: {queue_status['message_count']}")
            print(f"Consumers: {queue_status['consumer_count']}")
        
        # Comprehensive health check
        comprehensive = health_checker.comprehensive_check(conn)
        print(f"Comprehensive check: {comprehensive}")
```

**Health States**:
- `HEALTHY`: Connection is working normally
- `DEGRADED`: Connection is slow (>1s response time)
- `UNHEALTHY`: Connection failed or very slow (>2s)

---

## Best Practices

### 1. Always Use Connection Pools

```python
# ✅ Good: Use connection pool
with RabbitMQSyncConnectionPool(config) as pool:
    with pool.get_connection() as conn:
        channel = conn.channel()
        # Use channel...

# ❌ Bad: Create new connections for each operation
connection = pika.BlockingConnection(parameters)
```

### 2. Use Durable Queues for Important Data

```python
# ✅ Good: Durable queue with persistent messages
channel.queue_declare(queue='important_queue', durable=True)
channel.basic_publish(
    exchange='',
    routing_key='important_queue',
    body='Important message',
    properties=pika.BasicProperties(delivery_mode=2)
)
```

### 3. Implement Proper Error Handling

```python
try:
    with RabbitMQSyncConnectionPool(config) as pool:
        with pool.get_connection() as conn:
            channel = conn.channel()
            # Operations...
except ConnectionError as e:
    # Handle connection errors
    logger.error(f"Connection error: {e}")
    # Implement retry logic
except Exception as e:
    # Handle other errors
    logger.error(f"Error: {e}")
```

### 4. Use Manual Acknowledgments

```python
# ✅ Good: Manual acknowledgment with error handling
channel.basic_consume(
    queue='my_queue',
    on_message_callback=callback,
    auto_ack=False  # Manual acknowledgment
)
```

### 5. Set Appropriate Prefetch Count

```python
# ✅ Good: Fair dispatch
channel.basic_qos(prefetch_count=1)

# Use higher prefetch_count only if messages are processed very quickly
```

### 6. Close Channels and Connections Properly

```python
# ✅ Good: Use context managers
with pool.get_connection() as conn:
    channel = conn.channel()
    try:
        # Use channel...
    finally:
        channel.close()  # Always close channel
```

### 7. Use Appropriate Exchange Types

- **Direct**: Simple point-to-point routing
- **Fanout**: Broadcast to all consumers
- **Topic**: Pattern-based routing
- **Headers**: Complex header-based routing

### 8. Monitor Queue Lengths

```python
# Check queue status periodically
queue_status = health_checker.check_queue_status(channel, 'my_queue')
if queue_status['message_count'] > 1000:
    # Alert or scale consumers
    logger.warning(f"Queue backlog: {queue_status['message_count']}")
```

---

## Troubleshooting

### Connection Issues

**Problem**: Cannot connect to RabbitMQ

```python
# Check connection configuration
config = RabbitMQPoolConfig(
    host="localhost",
    port=5672,
    username="guest",
    password="guest",
    connection_attempts=3,
    retry_delay=2.0,
)
```

**Solutions**:
- Verify RabbitMQ is running: `rabbitmq-diagnostics ping`
- Check host, port, username, password
- Verify virtual host exists
- Check firewall/network connectivity

---

### Message Loss

**Problem**: Messages are lost after server restart

**Solution**: Use durable queues and persistent messages

```python
# ✅ Correct
channel.queue_declare(queue='my_queue', durable=True)
channel.basic_publish(
    exchange='',
    routing_key='my_queue',
    body='Message',
    properties=pika.BasicProperties(delivery_mode=2)  # Persistent
)
```

---

### Unfair Message Distribution

**Problem**: Some workers get more messages than others

**Solution**: Use `prefetch_count=1` for fair dispatch

```python
channel.basic_qos(prefetch_count=1)  # Fair dispatch
```

---

### High Memory Usage

**Problem**: Queue growing too large

**Solutions**:
- Set max queue length
- Add more consumers
- Use message TTL
- Implement dead letter queue for rejected messages

```python
channel.queue_declare(
    queue='my_queue',
    arguments={
        'x-max-length': 1000,  # Limit queue size
        'x-message-ttl': 3600000,  # 1 hour TTL
    }
)
```

---

### Connection Timeouts

**Problem**: Connections timing out

**Solution**: Adjust timeout settings

```python
config = RabbitMQPoolConfig(
    host="localhost",
    port=5672,
    timeout=60,  # Increase timeout
    socket_timeout=30,
    heartbeat=300,  # Keep connection alive
)
```

---

## Additional Resources

- [RabbitMQ Official Documentation](https://www.rabbitmq.com/documentation.html)
- [RabbitMQ Tutorials](https://www.rabbitmq.com/getstarted.html)
- [Pika Documentation](https://pika.readthedocs.io/)
- [aio-pika Documentation](https://aio-pika.readthedocs.io/)

---

## Example: Complete Application

See `rabbitmq_usage.py` for comprehensive examples covering:
- All exchange types (direct, fanout, topic, headers)
- All queue types and properties
- Publishing and consuming messages
- RPC pattern
- Health checks
- Error handling
- JSON message handling

Run examples:

```bash
# Start RabbitMQ (Docker example)
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management

# Run usage examples
python -m db_connections.scr.all_db_connectors.connectors.rabbitmq.rabbitmq_usage
```

