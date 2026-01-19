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
    
    def __init__(self, receiver=None, is_persistent=False, show_message=True, show_headers=False):
        self.message_count = 0
        self.receiver = receiver
        self.is_persistent = is_persistent
        self.show_message = show_message
        self.show_headers = show_headers
    
    def on_message(self, message: InboundMessage):
        """
        Callback when a message is received.
        
        Args:
            message: The received message
        """
        self.message_count += 1
        
        # Get topic
        topic = message.get_destination_name()
        
        # Output message header
        print(f"\n{'='*60}")
        print(f"Message #{self.message_count}")
        print(f"Topic: {topic}")
        
        # Conditionally display message headers
        if self.show_headers:
            print(f"\n--- Message Headers ---")
            
            # Application message ID
            app_msg_id = message.get_application_message_id()
            if app_msg_id:
                print(f"Application Message ID: {app_msg_id}")
            
            # Application message type
            app_msg_type = message.get_application_message_type()
            if app_msg_type:
                print(f"Application Message Type: {app_msg_type}")
            
            # Correlation ID
            correlation_id = message.get_correlation_id()
            if correlation_id:
                print(f"Correlation ID: {correlation_id}")
            
            # Sender ID
            sender_id = message.get_sender_id()
            if sender_id:
                print(f"Sender ID: {sender_id}")
            
            # Timestamp
            timestamp = message.get_sender_timestamp()
            if timestamp is not None:
                print(f"Sender Timestamp: {timestamp}")
            
            # Priority
            priority = message.get_priority()
            if priority is not None:
                print(f"Priority: {priority}")
            
            # Expiration
            expiration = message.get_expiration()
            if expiration is not None:
                print(f"Expiration: {expiration}")
            
            # Sequence number
            seq_num = message.get_sequence_number()
            if seq_num is not None:
                print(f"Sequence Number: {seq_num}")
            
            # Redelivered flag
            redelivered = message.is_redelivered()
            if redelivered is not None:
                print(f"Redelivered: {redelivered}")
            
            # User properties
            properties = message.get_properties()
            if properties:
                print(f"User Properties: {properties}")
        
        # Conditionally display message payload
        if self.show_message:
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
            
            print(f"Payload: {payload}")
        
        print(f"{'='*60}")
        print(f"Total messages received: {self.message_count}")
        
        # Acknowledge the message if in persistent mode (queue)
        if self.is_persistent and self.receiver:
            try:
                self.receiver.ack(message)
            except Exception as e:
                print(f"Warning: Failed to acknowledge message: {e}")


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
    parser.add_argument(
        '--queue-type',
        type=str,
        choices=['exclusive', 'non-exclusive'],
        default=os.getenv("SOLACE_QUEUE_TYPE", "exclusive"),
        help='Queue type: "exclusive" (only one consumer) or "non-exclusive" (multiple consumers for load balancing)'
    )
    parser.add_argument(
        '--ack',
        action='store_true',
        default=os.getenv("SOLACE_ACK", "").lower() in ['true', '1', 'yes'],
        help='Enable message acknowledgment for queue mode (removes messages from queue after processing)'
    )
    parser.add_argument(
        '--show-message',
        action=argparse.BooleanOptionalAction,
        default=os.getenv("SOLACE_SHOW_MESSAGE", "true").lower() in ['true', '1', 'yes'],
        help='Display message payload (default: enabled, use --no-show-message to disable)'
    )
    parser.add_argument(
        '--show-headers',
        action=argparse.BooleanOptionalAction,
        default=os.getenv("SOLACE_SHOW_HEADERS", "").lower() in ['true', '1', 'yes'],
        help='Display message headers (default: disabled, use --show-headers to enable)'
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
    QUEUE_TYPE = args.queue_type
    ACK_ENABLED = args.ack
    SHOW_MESSAGE = args.show_message
    SHOW_HEADERS = args.show_headers
    
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
        print(f"Queue: {QUEUE_NAME} ({QUEUE_TYPE})")
        print(f"Acknowledgment: {'enabled' if ACK_ENABLED else 'disabled'}")
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
    message_handler = None
    
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
            
            # Create message handler (no acknowledgment needed for direct messages)
            message_handler = MessageCounter(receiver=None, is_persistent=False, show_message=SHOW_MESSAGE, show_headers=SHOW_HEADERS)
            
            # Subscribe to topic
            print(f"\nSubscribing to topic: {TOPIC_SUBSCRIPTION}")
            topic_sub = TopicSubscription.of(TOPIC_SUBSCRIPTION)
            receiver.receive_async(message_handler)
            receiver.add_subscription(topic_sub)
            print("✓ Subscribed successfully!")
        else:
            # Create a persistent message receiver for queue subscriptions
            print(f"\nSetting up persistent message receiver for queue subscription...")
            print(f"Binding to queue: {QUEUE_NAME} ({QUEUE_TYPE})")
            if QUEUE_TYPE == 'exclusive':
                queue = Queue.durable_exclusive_queue(QUEUE_NAME)
            else:
                queue = Queue.durable_non_exclusive_queue(QUEUE_NAME)
            receiver = messaging_service.create_persistent_message_receiver_builder().build(queue)
            receiver.start()
            print("✓ Receiver started!")
            
            # Create message handler with receiver for acknowledgment
            message_handler = MessageCounter(receiver=receiver, is_persistent=ACK_ENABLED, show_message=SHOW_MESSAGE, show_headers=SHOW_HEADERS)
            
            # Set up message handler
            receiver.receive_async(message_handler)
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
