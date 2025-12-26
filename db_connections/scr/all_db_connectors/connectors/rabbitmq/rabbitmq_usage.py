"""
RabbitMQ Connection Pool Usage Examples

This file demonstrates various ways to use the RabbitMQ connection pools,
including all exchange types, queue types, and messaging patterns.
"""

import json

# Try to import pika for sync operations
try:
    import pika

    PIKA_AVAILABLE = True
except ImportError:
    PIKA_AVAILABLE = False
    pika = None

# Try to import aio_pika for async operations
try:
    import aio_pika

    AIO_PIKA_AVAILABLE = True
except ImportError:
    AIO_PIKA_AVAILABLE = False
    aio_pika = None

from db_connections.scr.all_db_connectors.connectors.rabbitmq import (
    RabbitMQSyncConnectionPool,
    RabbitMQAsyncConnectionPool,
    RabbitMQPoolConfig,
    RabbitMQHealthChecker,
)


# =============================================================================
# Example 1: Basic Synchronous Usage
# =============================================================================


def example_sync_basic():
    """Basic synchronous usage with context manager."""
    print("\n=== Example 1: Basic Sync Usage ===")

    config = RabbitMQPoolConfig(
        host="localhost",
        port=5672,
        username="guest",
        password="guest",
        virtual_host="/",
        min_connections=2,
        max_connections=10,
    )

    # Using context manager (recommended)
    with RabbitMQSyncConnectionPool(config) as pool:
        with pool.get_connection() as conn:
            channel = conn.channel()

            # Declare a queue
            channel.queue_declare(queue="basic_queue", durable=True)
            print("Declared queue: basic_queue")

            # Publish a message
            channel.basic_publish(
                exchange="", routing_key="basic_queue", body="Hello, RabbitMQ!"
            )
            print("Published message to basic_queue")

            # Close channel
            channel.close()


# =============================================================================
# Example 2: Direct Exchange (Point-to-Point Routing)
# =============================================================================


def example_direct_exchange():
    """Direct exchange routes messages to queues based on routing key."""
    print("\n=== Example 2: Direct Exchange ===")

    config = RabbitMQPoolConfig(host="localhost", port=5672)

    with RabbitMQSyncConnectionPool(config) as pool:
        with pool.get_connection() as conn:
            channel = conn.channel()

            # Declare a direct exchange
            exchange_name = "direct_logs"
            channel.exchange_declare(
                exchange=exchange_name, exchange_type="direct", durable=True
            )
            print(f"Declared direct exchange: {exchange_name}")

            # Declare queues with different routing keys
            queues = ["info", "warning", "error"]
            for queue_name in queues:
                channel.queue_declare(queue=queue_name, durable=True)
                # Bind queue to exchange with routing key
                channel.queue_bind(
                    exchange=exchange_name, queue=queue_name, routing_key=queue_name
                )
                print(f"Bound queue '{queue_name}' with routing_key '{queue_name}'")

            # Publish messages with different routing keys
            messages = {
                "info": "This is an info message",
                "warning": "This is a warning message",
                "error": "This is an error message",
            }

            import pika

            for routing_key, message in messages.items():
                channel.basic_publish(
                    exchange=exchange_name,
                    routing_key=routing_key,
                    body=message,
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # Make message persistent
                    ),
                )
                print(f"Published to {routing_key}: {message}")

            channel.close()


# =============================================================================
# Example 3: Fanout Exchange (Broadcast to All Queues)
# =============================================================================


def example_fanout_exchange():
    """Fanout exchange broadcasts messages to all bound queues."""
    print("\n=== Example 3: Fanout Exchange (Broadcast) ===")

    config = RabbitMQPoolConfig(host="localhost", port=5672)

    with RabbitMQSyncConnectionPool(config) as pool:
        with pool.get_connection() as conn:
            channel = conn.channel()

            # Declare a fanout exchange
            exchange_name = "notifications"
            channel.exchange_declare(
                exchange=exchange_name, exchange_type="fanout", durable=True
            )
            print(f"Declared fanout exchange: {exchange_name}")

            # Declare multiple queues (they all receive the message)
            queues = ["email_queue", "sms_queue", "push_queue"]
            for queue_name in queues:
                channel.queue_declare(queue=queue_name, durable=True)
                # Bind queue to fanout exchange
                # (routing_key is ignored for fanout)
                channel.queue_bind(exchange=exchange_name, queue=queue_name)
                print(f"Bound queue '{queue_name}' to fanout exchange")

            # Publish one message - it goes to ALL bound queues
            message = "New user registration!"
            channel.basic_publish(
                exchange=exchange_name,
                routing_key="",  # Ignored for fanout
                body=message,
            )
            print(
                f"Published message "
                f"(will be sent to all {len(queues)} queues): {message}"
            )

            channel.close()


# =============================================================================
# Example 4: Topic Exchange (Pattern-Based Routing)
# =============================================================================


def example_topic_exchange():
    """Topic exchange routes messages based on pattern matching."""
    print("\n=== Example 4: Topic Exchange (Pattern Matching) ===")

    config = RabbitMQPoolConfig(host="localhost", port=5672)

    with RabbitMQSyncConnectionPool(config) as pool:
        with pool.get_connection() as conn:
            channel = conn.channel()

            # Declare a topic exchange
            exchange_name = "topic_logs"
            channel.exchange_declare(
                exchange=exchange_name, exchange_type="topic", durable=True
            )
            print(f"Declared topic exchange: {exchange_name}")

            # Bind queues with different patterns
            # * matches one word, # matches zero or more words
            bindings = {
                "all_logs": "#",  # Receives all messages
                "error_logs": "*.error",  # Receives error messages from any service
                "user_events": "user.*",  # Receives all user-related events
                "user_errors": "user.error",  # Receives only user errors
            }

            for queue_name, routing_pattern in bindings.items():
                channel.queue_declare(queue=queue_name, durable=True)
                channel.queue_bind(
                    exchange=exchange_name,
                    queue=queue_name,
                    routing_key=routing_pattern,
                )
                print(f"Bound queue '{queue_name}' with pattern '{routing_pattern}'")

            # Publish messages with different routing keys
            messages = {
                "order.error": "Order processing error",
                "user.created": "New user created",
                "user.error": "User authentication error",
                "payment.success": "Payment processed successfully",
            }

            for routing_key, message in messages.items():
                channel.basic_publish(
                    exchange=exchange_name, routing_key=routing_key, body=message
                )
                print(f"Published '{routing_key}': {message}")

            channel.close()


# =============================================================================
# Example 5: Headers Exchange (Header-Based Routing)
# =============================================================================


def example_headers_exchange():
    """Headers exchange routes messages based on header attributes."""
    print("\n=== Example 5: Headers Exchange ===")

    config = RabbitMQPoolConfig(host="localhost", port=5672)

    with RabbitMQSyncConnectionPool(config) as pool:
        with pool.get_connection() as conn:
            channel = conn.channel()

            # Declare a headers exchange
            exchange_name = "headers_exchange"
            channel.exchange_declare(
                exchange=exchange_name, exchange_type="headers", durable=True
            )
            print(f"Declared headers exchange: {exchange_name}")

            # Declare queues with header bindings
            # x-match: 'all' means all headers must match,
            # 'any' means any header matches
            queues = {
                "urgent_queue": {
                    "x-match": "all",
                    "priority": "high",
                    "type": "urgent",
                },
                "notification_queue": {"x-match": "any", "priority": "high"},
                "email_queue": {"x-match": "all", "channel": "email"},
            }

            for queue_name, headers in queues.items():
                channel.queue_declare(queue=queue_name, durable=True)
                channel.queue_bind(
                    exchange=exchange_name, queue=queue_name, arguments=headers
                )
                print(f"Bound queue '{queue_name}' with headers: {headers}")

            # Publish messages with headers
            import pika

            message1 = "Urgent notification"
            channel.basic_publish(
                exchange=exchange_name,
                routing_key="",  # Ignored for headers exchange
                body=message1,
                properties=pika.BasicProperties(
                    headers={"priority": "high", "type": "urgent"}
                ),
            )
            print(f"Published message with headers: {message1}")

            channel.close()


# =============================================================================
# Example 6: Queue Types and Properties
# =============================================================================


def example_queue_types():
    """Demonstrate different queue types and properties."""
    print("\n=== Example 6: Queue Types and Properties ===")

    config = RabbitMQPoolConfig(host="localhost", port=5672)

    with RabbitMQSyncConnectionPool(config) as pool:
        with pool.get_connection() as conn:
            channel = conn.channel()

            # 1. Durable Queue (survives server restart)
            channel.queue_declare(
                queue="durable_queue",
                durable=True,  # Queue survives broker restart
                exclusive=False,
                auto_delete=False,
            )
            print("Created durable queue (survives server restart)")

            # 2. Exclusive Queue (only accessible by this connection)
            channel.queue_declare(
                queue="exclusive_queue",
                durable=False,
                exclusive=True,  # Automatically deleted when connection closes
                auto_delete=True,
            )
            print("Created exclusive queue (connection-specific)")

            # 3. Auto-delete Queue (deleted when no longer used)
            channel.queue_declare(
                queue="temp_queue",
                durable=False,
                exclusive=False,
                auto_delete=True,  # Deleted when last consumer unsubscribes
            )
            print("Created auto-delete queue")

            # 4. Queue with TTL (Time To Live)
            channel.queue_declare(
                queue="ttl_queue",
                durable=True,
                arguments={"x-message-ttl": 60000},  # Messages expire after 60 seconds
            )
            print("Created queue with message TTL (60 seconds)")

            # 5. Queue with Max Length
            channel.queue_declare(
                queue="limited_queue",
                durable=True,
                arguments={"x-max-length": 100},  # Max 100 messages
            )
            print("Created queue with max length (100 messages)")

            # 6. Dead Letter Queue (for failed messages)
            channel.queue_declare(queue="dlq", durable=True)
            channel.queue_declare(
                queue="main_queue",
                durable=True,
                arguments={
                    "x-dead-letter-exchange": "",  # Use default exchange
                    "x-dead-letter-routing-key": "dlq",  # Route to DLQ
                },
            )
            print("Created queue with dead letter queue configuration")

            channel.close()


# =============================================================================
# Example 7: Message Publishing with Properties
# =============================================================================


def example_message_properties():
    """Demonstrate message properties and delivery options."""
    print("\n=== Example 7: Message Properties ===")

    config = RabbitMQPoolConfig(host="localhost", port=5672)

    with RabbitMQSyncConnectionPool(config) as pool:
        with pool.get_connection() as conn:
            channel = conn.channel()

            queue_name = "properties_queue"
            channel.queue_declare(queue=queue_name, durable=True)

            import pika

            # Message with persistence
            channel.basic_publish(
                exchange="",
                routing_key=queue_name,
                body="Persistent message",
                properties=pika.BasicProperties(
                    delivery_mode=2,  # 1=non-persistent, 2=persistent
                ),
            )
            print("Published persistent message")

            # Message with expiration
            channel.basic_publish(
                exchange="",
                routing_key=queue_name,
                body="Message with expiration",
                properties=pika.BasicProperties(
                    expiration="30000",  # Expires in 30 seconds
                ),
            )
            print("Published message with expiration (30 seconds)")

            # Message with priority
            channel.basic_publish(
                exchange="",
                routing_key=queue_name,
                body="High priority message",
                properties=pika.BasicProperties(
                    priority=10,  # Higher priority (0-255)
                ),
            )
            print("Published high priority message")

            # Message with custom headers
            channel.basic_publish(
                exchange="",
                routing_key=queue_name,
                body="Message with headers",
                properties=pika.BasicProperties(
                    headers={
                        "user_id": "12345",
                        "action": "login",
                        "timestamp": "2024-01-01T00:00:00Z",
                    }
                ),
            )
            print("Published message with custom headers")

            # Message with correlation ID (for RPC)
            channel.basic_publish(
                exchange="",
                routing_key=queue_name,
                body="RPC request",
                properties=pika.BasicProperties(
                    correlation_id="req-123",
                    reply_to="response_queue",
                ),
            )
            print("Published RPC request with correlation ID")

            channel.close()


# =============================================================================
# Example 8: Message Consumption
# =============================================================================


def example_message_consumption():
    """Demonstrate message consumption patterns."""
    print("\n=== Example 8: Message Consumption ===")

    config = RabbitMQPoolConfig(host="localhost", port=5672)

    with RabbitMQSyncConnectionPool(config) as pool:
        with pool.get_connection() as conn:
            channel = conn.channel()

            queue_name = "consumer_queue"
            channel.queue_declare(queue=queue_name, durable=True)

            # Publish some messages first
            for i in range(5):
                channel.basic_publish(
                    exchange="", routing_key=queue_name, body=f"Message {i + 1}"
                )
            print(f"Published 5 messages to {queue_name}")

            # Define callback for consuming messages
            def callback(ch, method, properties, body):
                print(f"Received: {body.decode()}")
                # Acknowledge message
                ch.basic_ack(delivery_tag=method.delivery_tag)

            # Set up consumer with prefetch (fair dispatch)
            channel.basic_qos(prefetch_count=1)  # Don't dispatch new message until ack
            channel.basic_consume(
                queue=queue_name,
                on_message_callback=callback,
                auto_ack=False,  # Manual acknowledgment
            )

            print("Starting to consume messages (Ctrl+C to stop)...")
            print("Note: This example would run indefinitely in real usage")
            # channel.start_consuming()  # Uncomment to actually consume

            channel.close()


# =============================================================================
# Example 9: RPC Pattern (Request-Reply)
# =============================================================================


def example_rpc_pattern():
    """Demonstrate RPC (Remote Procedure Call) pattern."""
    print("\n=== Example 9: RPC Pattern ===")

    config = RabbitMQPoolConfig(host="localhost", port=5672)

    with RabbitMQSyncConnectionPool(config) as pool:
        with pool.get_connection() as conn:
            channel = conn.channel()

            # Server side: declare RPC queue
            rpc_queue = "rpc_queue"
            channel.queue_declare(queue=rpc_queue, durable=True)

            # Client side: declare callback queue for response
            result = channel.queue_declare(queue="", exclusive=True)
            callback_queue = result.method.queue

            correlation_id = "req-001"
            response = None

            def on_response(ch, method, properties, body):
                nonlocal response
                if properties.correlation_id == correlation_id:
                    response = body.decode()
                    ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_consume(
                queue=callback_queue, on_message_callback=on_response, auto_ack=False
            )

            # Send RPC request
            channel.basic_publish(
                exchange="",
                routing_key=rpc_queue,
                body="Calculate: 2 + 2",
                properties=pika.BasicProperties(
                    reply_to=callback_queue,
                    correlation_id=correlation_id,
                ),
            )
            print(f"Sent RPC request with correlation_id: {correlation_id}")

            # Note: In real usage, you'd wait for response
            # while response is None:
            #     conn.process_data_events(time_limit=1)

            channel.close()


# =============================================================================
# Example 10: Asynchronous Usage
# =============================================================================


async def example_async_basic():
    """Basic asynchronous usage."""
    print("\n=== Example 10: Async Usage ===")

    config = RabbitMQPoolConfig(host="localhost", port=5672)

    async with RabbitMQAsyncConnectionPool(config) as pool:
        async with pool.get_connection() as conn:
            channel = await conn.channel()

            # Declare queue
            queue = await channel.declare_queue("async_queue", durable=True)
            print("Declared async_queue")

            # Publish message
            await channel.default_exchange.publish(
                aio_pika.Message(b"Hello from async!"), routing_key="async_queue"
            )
            print("Published message asynchronously")

            # Consume messages
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        print(f"Received: {message.body.decode()}")
                        break  # Just process one message for example

            await channel.close()


# =============================================================================
# Example 11: Health Checks
# =============================================================================


def example_health_checks():
    """Demonstrate health checking."""
    print("\n=== Example 11: Health Checks ===")

    config = RabbitMQPoolConfig(host="localhost", port=5672)
    health_checker = RabbitMQHealthChecker(config)

    with RabbitMQSyncConnectionPool(config) as pool:
        with pool.get_connection() as conn:
            # Check connection health
            health_status = health_checker.check_health(conn)
            print(f"Connection Health: {health_status.state}")
            print(f"Response Time: {health_status.response_time_ms}ms")
            print(f"Message: {health_status.message}")

            # Check queue status
            channel = conn.channel()
            channel.queue_declare(queue="health_check_queue", durable=True)
            queue_status = health_checker.check_queue_status(
                channel, "health_check_queue"
            )
            if queue_status:
                print(f"Queue Status: {queue_status}")

            # Check exchange status
            channel.exchange_declare(
                exchange="health_check_exchange", exchange_type="direct"
            )
            exchange_status = health_checker.check_exchange_status(
                channel, "health_check_exchange"
            )
            if exchange_status:
                print(f"Exchange Status: {exchange_status}")

            # Comprehensive check
            comprehensive = health_checker.comprehensive_check(conn)
            print(f"Comprehensive Check: {comprehensive.keys()}")

            channel.close()


# =============================================================================
# Example 12: Connection Pool Status
# =============================================================================


def example_pool_status():
    """Demonstrate pool status and metrics."""
    print("\n=== Example 12: Pool Status ===")

    config = RabbitMQPoolConfig(
        host="localhost",
        port=5672,
        min_connections=2,
        max_connections=10,
    )

    with RabbitMQSyncConnectionPool(config) as pool:
        # Get pool status
        status = pool.pool_status()
        print(f"Pool Status: {status}")
        print(f"Total Connections: {status.get('total_connections')}")
        print(f"Active Connections: {status.get('active_connections')}")
        print(f"Idle Connections: {status.get('idle_connections')}")
        print(f"Max Connections: {status.get('max_connections')}")

        # Get metrics
        metrics = pool.get_metrics()
        print("\nMetrics:")
        print(f"  Total Requests: {metrics.total_requests}")
        print(f"  Successful Requests: {metrics.successful_requests}")
        print(f"  Failed Requests: {metrics.failed_requests}")
        print(f"  Average Response Time: {metrics.avg_response_time_ms}ms")


# =============================================================================
# Example 13: Error Handling and Retries
# =============================================================================


def example_error_handling():
    """Demonstrate error handling patterns."""
    print("\n=== Example 13: Error Handling ===")

    config = RabbitMQPoolConfig(
        host="localhost",
        port=5672,
        connection_attempts=3,
        retry_delay=2.0,
    )

    try:
        with RabbitMQSyncConnectionPool(config) as pool:
            with pool.get_connection() as conn:
                channel = conn.channel()

                try:
                    # Attempt to declare queue
                    channel.queue_declare(queue="test_queue", durable=True)
                    print("Queue declared successfully")

                except Exception as e:
                    print(f"Error declaring queue: {e}")
                    # Handle error appropriately

                channel.close()

    except Exception as e:
        print(f"Connection error: {e}")
        # Implement retry logic or fallback behavior


# =============================================================================
# Example 14: JSON Message Publishing
# =============================================================================


def example_json_messages():
    """Demonstrate publishing and consuming JSON messages."""
    print("\n=== Example 14: JSON Messages ===")

    config = RabbitMQPoolConfig(host="localhost", port=5672)

    with RabbitMQSyncConnectionPool(config) as pool:
        with pool.get_connection() as conn:
            channel = conn.channel()

            queue_name = "json_queue"
            channel.queue_declare(queue=queue_name, durable=True)

            # Publish JSON message
            data = {
                "user_id": 12345,
                "action": "login",
                "timestamp": "2024-01-01T00:00:00Z",
                "metadata": {"ip": "192.168.1.1", "browser": "Chrome"},
            }

            import pika

            channel.basic_publish(
                exchange="",
                routing_key=queue_name,
                body=json.dumps(data),
                properties=pika.BasicProperties(
                    content_type="application/json",
                    delivery_mode=2,  # Persistent
                ),
            )
            print(f"Published JSON message: {data}")

            # Consume JSON message (callback example)
            def json_callback(ch, method, properties, body):
                if properties.content_type == "application/json":
                    data = json.loads(body.decode())
                    print(f"Received JSON: {data}")
                    ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_consume(
                queue=queue_name, on_message_callback=json_callback, auto_ack=False
            )

            print("JSON consumer set up (would process messages in real usage)")
            channel.close()


# =============================================================================
# Main Execution
# =============================================================================

if __name__ == "__main__":
    # Note: These examples require a running RabbitMQ instance
    # Make sure RabbitMQ is running on localhost:5672

    print("RabbitMQ Usage Examples")
    print("=" * 60)
    print("\nNote: These examples require a running RabbitMQ server.")
    print("Start RabbitMQ before running these examples.\n")

    # Uncomment examples to run them:

    # example_sync_basic()
    # example_direct_exchange()
    # example_fanout_exchange()
    # example_topic_exchange()
    # example_headers_exchange()
    # example_queue_types()
    # example_message_properties()
    # example_message_consumption()
    # example_rpc_pattern()
    # asyncio.run(example_async_basic())
    # example_health_checks()
    # example_pool_status()
    # example_error_handling()
    # example_json_messages()

    print("\nExamples are ready to use. Uncomment desired examples to run them.")
