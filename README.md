# Solace Message Consumer

A Python program that consumes messages from Solace topics and queues, displays the messages, and tracks the message count.

## Features

- Connects to Solace PubSub+ broker
- Subscribes to configurable topics or queues
- Displays received messages with topic/queue information
- Tracks and displays message count
- Clean shutdown with summary statistics

## Prerequisites

- Python 3.7 or higher
- Access to a Solace PubSub+ broker (local or cloud)

## Installation

1. Clone this repository:
```bash
git clone https://github.com/stevena007/solace-message-consumer.git
cd solace-message-consumer
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage

Run the consumer with default settings:
```bash
python solace_consumer.py
```

### Command-Line Arguments

Configure the consumer using command-line parameters:

**Topic Subscription (Default):**
```bash
python solace_consumer.py --host tcp://your-broker:55555 --vpn your-vpn --username your-username --password your-password --topic "your/topic/>"
```

**Queue Subscription:**
```bash
python solace_consumer.py --mode queue --queue your-queue-name --host tcp://your-broker:55555 --vpn your-vpn --username your-username --password your-password
```

**Queue Subscription with Message Acknowledgment:**
```bash
python solace_consumer.py --mode queue --queue your-queue-name --ack --host tcp://your-broker:55555 --vpn your-vpn --username your-username --password your-password
```

**Queue Subscription (Non-Exclusive for Load Balancing):**
```bash
python solace_consumer.py --mode queue --queue your-queue-name --queue-type non-exclusive --host tcp://your-broker:55555 --vpn your-vpn --username your-username --password your-password
```

**Display Message Headers:**
```bash
python solace_consumer.py --show-headers --topic "your/topic/>"
```

**Hide Message Payload, Show Headers Only:**
```bash
# Using command-line flag
python solace_consumer.py --no-show-message --show-headers --topic "your/topic/>"

# Or using environment variable
export SOLACE_SHOW_MESSAGE=false
python solace_consumer.py --show-headers --topic "your/topic/>"
```

**Display Both Message and Headers:**
```bash
python solace_consumer.py --show-message --show-headers --topic "your/topic/>"
```

Available parameters:
- `--host`: Broker host and port (default: `tcp://localhost:55555`)
- `--vpn`: Message VPN name (default: `default`)
- `--username`: Authentication username (default: `default`)
- `--password`: Authentication password (default: `default`)
- `--mode`: Subscription mode - `topic` or `queue` (default: `topic`)
- `--topic`: Topic subscription pattern (default: `solace/samples/>`, used when mode is `topic`)
- `--queue`: Queue name (required when mode is `queue`)
- `--queue-type`: Queue type - `exclusive` or `non-exclusive` (default: `exclusive`)
- `--ack`: Enable message acknowledgment for queue mode (removes messages from queue after processing)
- `--show-message` / `--no-show-message`: Display message payload (default: enabled)
- `--show-headers` / `--no-show-headers`: Display message headers including correlation ID, timestamp, priority, etc. (default: disabled)

To see all available options:
```bash
python solace_consumer.py --help
```

### Environment Variables

You can also configure the consumer using environment variables (command-line arguments take precedence):

- `SOLACE_HOST`: Broker host and port
- `SOLACE_VPN`: Message VPN name
- `SOLACE_USERNAME`: Authentication username
- `SOLACE_PASSWORD`: Authentication password
- `SOLACE_MODE`: Subscription mode (`topic` or `queue`)
- `SOLACE_TOPIC`: Topic subscription pattern
- `SOLACE_QUEUE`: Queue name
- `SOLACE_QUEUE_TYPE`: Queue type (`exclusive` or `non-exclusive`)
- `SOLACE_ACK`: Enable message acknowledgment (`true`, `1`, or `yes` to enable)
- `SOLACE_SHOW_MESSAGE`: Display message payload (`true`, `1`, or `yes` to enable, enabled by default)
- `SOLACE_SHOW_HEADERS`: Display message headers (`true`, `1`, or `yes` to enable)

**Example (Topic):**
```bash
export SOLACE_HOST="tcp://your-broker:55555"
export SOLACE_VPN="your-vpn"
export SOLACE_USERNAME="your-username"
export SOLACE_PASSWORD="your-password"
export SOLACE_TOPIC="your/topic/>"
python solace_consumer.py
```

**Example (Queue with Non-Exclusive Type and Acknowledgment):**
```bash
export SOLACE_HOST="tcp://your-broker:55555"
export SOLACE_VPN="your-vpn"
export SOLACE_USERNAME="your-username"
export SOLACE_PASSWORD="your-password"
export SOLACE_MODE="queue"
export SOLACE_QUEUE="your-queue-name"
export SOLACE_QUEUE_TYPE="non-exclusive"
export SOLACE_ACK="true"
python solace_consumer.py
```


### Running with Docker (Solace Broker)

If you don't have a Solace broker, you can run one locally using Docker:

```bash
docker run -d -p 8080:8080 -p 55555:55555 -p 8008:8008 -p 1883:1883 -p 8000:8000 -p 5672:5672 -p 9000:9000 -p 2222:2222 --shm-size=2g --env username_admin_globalaccesslevel=admin --env username_admin_password=admin --name=solace solace/solace-pubsub-standard
```

Then publish test messages to see the consumer in action.

## Message Acknowledgment

When consuming messages from a queue, you can enable message acknowledgment using the `--ack` flag. This is important for ensuring messages are properly removed from the queue after processing.

**Without acknowledgment (default):**
- Messages are received but remain in the queue
- Messages may be redelivered if the consumer disconnects
- Useful for testing or when you want to preserve messages

**With acknowledgment (`--ack` flag):**
- Messages are acknowledged after successful receipt
- Acknowledged messages are removed from the queue
- Ensures messages are processed only once
- Recommended for production use

**Example:**
```bash
python solace_consumer.py --mode queue --queue myQueue --ack
```

**Note:** Message acknowledgment is only applicable to queue mode. Topic mode uses direct messages which don't require acknowledgment.

## Output Format

The consumer displays messages in the following format:

**Default (message payload only):**
```
============================================================
Message #1
Topic: solace/samples/test
Payload: Hello, Solace!
============================================================
Total messages received: 1
```

**With headers enabled (`--show-headers`):**
```
============================================================
Message #1
Topic: solace/samples/test

--- Message Headers ---
Correlation ID: abc123
Sender Timestamp: 1234567890
Priority: 4
User Properties: {'key1': 'value1', 'key2': 'value2'}
Payload: Hello, Solace!
============================================================
Total messages received: 1
```

**Headers only (with `SOLACE_SHOW_MESSAGE=false` environment variable):**
```
============================================================
Message #1
Topic: solace/samples/test

--- Message Headers ---
Correlation ID: abc123
Sender Timestamp: 1234567890
Priority: 4
============================================================
Total messages received: 1
```

## Stopping the Consumer

Press `Ctrl+C` to gracefully stop the consumer. It will display the total message count before exiting.

## Example

**Topic Subscription Example:**
```bash
$ python solace_consumer.py
Solace Message Consumer
============================================================
Connecting to: tcp://localhost:55555
VPN: default
Username: *******
Mode: topic
Topic: solace/samples/>
============================================================

Connecting to Solace broker...
✓ Connected successfully!

Setting up direct message receiver for topic subscription...
✓ Receiver started!

Subscribing to topic: solace/samples/>
✓ Subscribed successfully!

============================================================
Waiting for messages... (Press Ctrl+C to exit)
============================================================

============================================================
Message #1
Topic: solace/samples/test
Payload: Hello from Solace!
============================================================
Total messages received: 1
```

**Queue Subscription Example:**
```bash
$ python solace_consumer.py --mode queue --queue myQueue
Solace Message Consumer
============================================================
Connecting to: tcp://localhost:55555
VPN: default
Username: *******
Mode: queue
Queue: myQueue (exclusive)
============================================================

Connecting to Solace broker...
✓ Connected successfully!

Setting up persistent message receiver for queue subscription...
✓ Receiver started!

Binding to queue: myQueue (exclusive)
✓ Bound to queue successfully!

============================================================
Waiting for messages... (Press Ctrl+C to exit)
============================================================

============================================================
Message #1
Topic: solace/samples/test
Payload: Hello from Queue!
============================================================
Total messages received: 1
```

## License

MIT