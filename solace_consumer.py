#!/usr/bin/env python3
"""
Solace Message Consumer
Connects to a Solace broker and consumes messages from topics or queues.
Outputs each message and tracks the message count.
"""

import sys
import time
import os
import argparse
from solace.messaging.messaging_service import MessagingService
from solace.messaging.resources.topic_subscription import TopicSubscription
from solace.messaging.resources.queue import Queue
from solace.messaging.receiver.message_receiver import MessageHandler, InboundMessage
from solace.messaging.config.solace_properties import service_properties, transport_layer_properties, authentication_properties
from solace.messaging.errors.pubsubplus_client_error import PubSubPlusClientError


class MessageCounter(MessageHandler):
    """Message handler that counts and displays messages."""
    
    def __init__(self):
        self.message_count = 0
    
    def on_message(self, message: InboundMessage):
        """
        Callback when a message is received.
        
        Args:
            message: The received message
        """
        self.message_count += 1
        
        # Get message payload
        payload_str = message.get_payload_as_string()
        if payload_str is not None:
            payload = payload_str
        else:
            # If not a string, get bytes and decode or represent them
            payload_bytes = message.get_payload_as_bytes()
            try:
                payload = payload_bytes.decode('utf-8') if payload_bytes else "[empty]"
            except (UnicodeDecodeError, AttributeError):
                payload = repr(payload_bytes)
        
        # Get topic
        topic = message.get_destination_name()
        
        # Output message details
        print(f"\n{'='*60}")
        print(f"Message #{self.message_count}")
        print(f"Topic: {topic}")
        print(f"Payload: {payload}")
        print(f"{'='*60}")
        print(f"Total messages received: {self.message_count}")


def main():
    """Main function to set up and run the Solace message consumer."""
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description='Solace Message Consumer - Consume messages from Solace topics or queues',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '--host',
        type=str,
        default=os.getenv("SOLACE_HOST", "tcp://localhost:55555"),
        help='Solace broker host and port'
    )
    parser.add_argument(
        '--vpn',
        type=str,
        default=os.getenv("SOLACE_VPN", "default"),
        help='Solace message VPN name'
    )
    parser.add_argument(
        '--username',
        type=str,
        default=os.getenv("SOLACE_USERNAME", "default"),
        help='Authentication username'
    )
    parser.add_argument(
        '--password',
        type=str,
        default=os.getenv("SOLACE_PASSWORD", "default"),
        help='Authentication password'
    )
    parser.add_argument(
        '--topic',
        type=str,
        default=os.getenv("SOLACE_TOPIC", "solace/samples/>"),
        help='Topic subscription pattern (used when mode is "topic")'
    )
    parser.add_argument(
        '--mode',
        type=str,
        choices=['topic', 'queue'],
        default=os.getenv("SOLACE_MODE", "topic"),
        help='Subscription mode: "topic" for topic subscriptions or "queue" for queue subscriptions'
    )
    parser.add_argument(
        '--queue',
        type=str,
        default=os.getenv("SOLACE_QUEUE", ""),
        help='Queue name (required when mode is "queue")'
    )
    
    args = parser.parse_args()
    
    # Configuration from command-line arguments
    SOLACE_HOST = args.host
    SOLACE_VPN = args.vpn
    SOLACE_USERNAME = args.username
    SOLACE_PASSWORD = args.password
    TOPIC_SUBSCRIPTION = args.topic
    SUBSCRIPTION_MODE = args.mode
    QUEUE_NAME = args.queue
    
    # Validate queue mode
    if SUBSCRIPTION_MODE == 'queue' and not QUEUE_NAME:
        parser.error("--queue is required when --mode is 'queue'")
    
    print("Solace Message Consumer")
    print("=" * 60)
    print(f"Connecting to: {SOLACE_HOST}")
    print(f"VPN: {SOLACE_VPN}")
    print(f"Username: {'*' * len(SOLACE_USERNAME) if SOLACE_USERNAME else '[none]'}")
    print(f"Mode: {SUBSCRIPTION_MODE}")
    if SUBSCRIPTION_MODE == 'topic':
        print(f"Topic: {TOPIC_SUBSCRIPTION}")
    else:
        print(f"Queue: {QUEUE_NAME}")
    print("=" * 60)
    
    # Build broker properties
    broker_props = {
        transport_layer_properties.HOST: SOLACE_HOST,
        service_properties.VPN_NAME: SOLACE_VPN,
        authentication_properties.SCHEME_BASIC_USER_NAME: SOLACE_USERNAME,
        authentication_properties.SCHEME_BASIC_PASSWORD: SOLACE_PASSWORD
    }
    
    # Track resources for cleanup
    receiver = None
    messaging_service = None
    message_handler = MessageCounter()
    
    try:
        # Create messaging service
        messaging_service = MessagingService.builder().from_properties(broker_props).build()
        
        # Connect to the messaging service
        print("\nConnecting to Solace broker...")
        messaging_service.connect()
        print("✓ Connected successfully!")
        
        # Create appropriate receiver based on mode
        if SUBSCRIPTION_MODE == 'topic':
            # Create a direct message receiver for topic subscriptions
            print("\nSetting up direct message receiver for topic subscription...")
            receiver = messaging_service.create_direct_message_receiver_builder().build()
            receiver.start()
            print("✓ Receiver started!")
            
            # Subscribe to topic
            print(f"\nSubscribing to topic: {TOPIC_SUBSCRIPTION}")
            topic_sub = TopicSubscription.of(TOPIC_SUBSCRIPTION)
            receiver.receive_async(message_handler)
            receiver.add_subscription(topic_sub)
            print("✓ Subscribed successfully!")
        else:
            # Create a persistent message receiver for queue subscriptions
            print("\nSetting up persistent message receiver for queue subscription...")
            receiver = messaging_service.create_persistent_message_receiver_builder().build()
            receiver.start()
            print("✓ Receiver started!")
            
            # Bind to queue
            print(f"\nBinding to queue: {QUEUE_NAME}")
            queue = Queue.durable_exclusive_queue(QUEUE_NAME)
            receiver.receive_async(message_handler)
            receiver.add_subscription(queue)
            print("✓ Bound to queue successfully!")
        
        print("\n" + "=" * 60)
        print("Waiting for messages... (Press Ctrl+C to exit)")
        print("=" * 60)
        
        # Keep the application running
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n\nShutting down...")
        
        # Cleanup
        print("\nCleaning up...")
        if receiver:
            try:
                receiver.terminate()
            except Exception as e:
                print(f"Warning: Error terminating receiver: {e}")
        
        if messaging_service:
            try:
                messaging_service.disconnect()
            except Exception as e:
                print(f"Warning: Error disconnecting: {e}")
        
        print(f"✓ Total messages received: {message_handler.message_count}")
        print("✓ Disconnected successfully!")
        
    except PubSubPlusClientError as e:
        print(f"\n✗ Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
