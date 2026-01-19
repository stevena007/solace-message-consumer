# Solace Message Consumer

A Python program that consumes messages from a Solace topic, displays the messages, and tracks the message count.

## Features

- Connects to Solace PubSub+ broker
- Subscribes to configurable topics
- Displays received messages with topic information
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

### Configuration

Edit the configuration variables in `solace_consumer.py` to match your Solace broker setup:

- `SOLACE_HOST`: Broker host and port (default: `tcp://localhost:55555`)
- `SOLACE_VPN`: Message VPN name (default: `default`)
- `SOLACE_USERNAME`: Authentication username (default: `default`)
- `SOLACE_PASSWORD`: Authentication password (default: `default`)
- `TOPIC_SUBSCRIPTION`: Topic subscription pattern (default: `solace/samples/>`)

### Running with Docker (Solace Broker)

If you don't have a Solace broker, you can run one locally using Docker:

```bash
docker run -d -p 8080:8080 -p 55555:55555 -p 8008:8008 -p 1883:1883 -p 8000:8000 -p 5672:5672 -p 9000:9000 -p 2222:2222 --shm-size=2g --env username_admin_globalaccesslevel=admin --env username_admin_password=admin --name=solace solace/solace-pubsub-standard
```

Then publish test messages to see the consumer in action.

## Output Format

The consumer displays messages in the following format:

```
============================================================
Message #1
Topic: solace/samples/test
Payload: Hello, Solace!
============================================================
Total messages received: 1
```

## Stopping the Consumer

Press `Ctrl+C` to gracefully stop the consumer. It will display the total message count before exiting.

## Example

```bash
$ python solace_consumer.py
Solace Message Consumer
============================================================
Connecting to: tcp://localhost:55555
VPN: default
Username: default
Topic: solace/samples/>
============================================================

Connecting to Solace broker...
✓ Connected successfully!

Setting up message receiver...
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

## License

MIT