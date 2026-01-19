#!/usr/bin/env python3
"""
Solace Message Consumer
Connects to a Solace broker and consumes messages from a topic.
Outputs each message and tracks the message count.
"""

import sys
import time
import os
from solace.messaging.messaging_service import MessagingService
from solace.messaging.resources.topic_subscription import TopicSubscription
from solace.messaging.receiver.message_receiver import MessageHandler, InboundMessage
from solace.messaging.config.solace_properties import service_properties
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
        payload = payload_str if payload_str else message.get_payload_as_bytes()
        
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
    
    # Configuration - read from environment variables with fallback defaults
    SOLACE_HOST = os.getenv("SOLACE_HOST", "tcp://localhost:55555")
    SOLACE_VPN = os.getenv("SOLACE_VPN", "default")
    SOLACE_USERNAME = os.getenv("SOLACE_USERNAME", "default")
    SOLACE_PASSWORD = os.getenv("SOLACE_PASSWORD", "default")
    TOPIC_SUBSCRIPTION = os.getenv("SOLACE_TOPIC", "solace/samples/>")
    
    print("Solace Message Consumer")
    print("=" * 60)
    print(f"Connecting to: {SOLACE_HOST}")
    print(f"VPN: {SOLACE_VPN}")
    print(f"Username: {SOLACE_USERNAME}")
    print(f"Topic: {TOPIC_SUBSCRIPTION}")
    print("=" * 60)
    
    # Build broker properties
    broker_props = {
        service_properties.HOST: SOLACE_HOST,
        service_properties.VPN_NAME: SOLACE_VPN,
        service_properties.AUTHENTICATION_BASIC_USERNAME: SOLACE_USERNAME,
        service_properties.AUTHENTICATION_BASIC_PASSWORD: SOLACE_PASSWORD
    }
    
    try:
        # Create messaging service
        messaging_service = MessagingService.builder().from_properties(broker_props).build()
        
        # Connect to the messaging service
        print("\nConnecting to Solace broker...")
        messaging_service.connect()
        print("✓ Connected successfully!")
        
        # Create a direct message receiver
        print("\nSetting up message receiver...")
        direct_receiver = messaging_service.create_direct_message_receiver_builder().build()
        direct_receiver.start()
        print("✓ Receiver started!")
        
        # Create message handler
        message_handler = MessageCounter()
        
        # Subscribe to topic
        print(f"\nSubscribing to topic: {TOPIC_SUBSCRIPTION}")
        topic_sub = TopicSubscription.of(TOPIC_SUBSCRIPTION)
        direct_receiver.receive_async(message_handler)
        direct_receiver.add_subscription(topic_sub)
        print("✓ Subscribed successfully!")
        
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
        direct_receiver.terminate()
        messaging_service.disconnect()
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
